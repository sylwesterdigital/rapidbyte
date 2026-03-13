//! Arrow Flight server for dry-run preview replay.
//!
//! Validates signed tickets and streams `RecordBatches` from the preview spool.

use std::pin::Pin;
use std::sync::Arc;

use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::spool::{PreviewKey, PreviewSpool};
use crate::ticket::TicketVerifier;

pub struct PreviewFlightService {
    spool: Arc<RwLock<PreviewSpool>>,
    verifier: Arc<TicketVerifier>,
}

impl PreviewFlightService {
    pub fn new(spool: Arc<RwLock<PreviewSpool>>, signing_key: &[u8]) -> Self {
        Self {
            spool,
            verifier: Arc::new(TicketVerifier::new(signing_key)),
        }
    }

    #[must_use]
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    async fn lookup_stream(
        &self,
        payload: &crate::ticket::TicketPayload,
    ) -> Result<(Vec<arrow::record_batch::RecordBatch>, u64, u64), Status> {
        let spool = self.spool.read().await;
        let key = PreviewKey {
            run_id: payload.run_id.clone(),
            task_id: payload.task_id.clone(),
            lease_epoch: payload.lease_epoch,
        };
        let dry_run = spool
            .get(&key)
            .ok_or_else(|| Status::not_found("Preview not found or expired"))?;
        dry_run
            .streams
            .iter()
            .find(|stream| stream.stream_name == payload.stream_name)
            .map(|stream| {
                (
                    stream.batches.clone(),
                    stream.total_rows,
                    stream.total_bytes,
                )
            })
            .ok_or_else(|| Status::not_found("Preview stream not found"))
    }
}

type BoxedStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl FlightService for PreviewFlightService {
    type HandshakeStream = BoxedStream<HandshakeResponse>;
    type ListFlightsStream = BoxedStream<FlightInfo>;
    type DoGetStream = BoxedStream<FlightData>;
    type DoPutStream = BoxedStream<PutResult>;
    type DoActionStream = BoxedStream<arrow_flight::Result>;
    type ListActionsStream = BoxedStream<ActionType>;
    type DoExchangeStream = BoxedStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_bytes = request.into_inner().ticket;
        let payload = self
            .verifier
            .verify(&ticket_bytes)
            .map_err(|e| Status::unauthenticated(format!("Invalid ticket: {e}")))?;

        let (batches, _, _) = self.lookup_stream(&payload).await?;

        if batches.is_empty() {
            let stream = tokio_stream::empty();
            return Ok(Response::new(Box::pin(stream)));
        }

        let schema = batches[0].schema();

        // IPC encoding happens outside the spool lock
        let options = IpcWriteOptions::default();
        let schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();

        let mut flight_data_vec = vec![schema_flight_data];
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        for batch in &batches {
            let (encoded_dictionaries, encoded_batch) = data_gen
                .encoded_batch(batch, &mut dictionary_tracker, &options)
                .map_err(|e| Status::internal(format!("Failed to encode batch: {e}")))?;

            for dict in encoded_dictionaries {
                let flight_data = FlightData::from(dict);
                flight_data_vec.push(flight_data);
            }
            let flight_data = FlightData::from(encoded_batch);
            flight_data_vec.push(flight_data);
        }

        let stream = tokio_stream::iter(flight_data_vec.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let payload = self
            .verifier
            .verify(&descriptor.cmd)
            .map_err(|e| Status::unauthenticated(format!("Invalid ticket: {e}")))?;

        let (batches, total_rows, total_bytes) = self.lookup_stream(&payload).await?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(descriptor.cmd.clone()));

        let info = if let Some(first_batch) = batches.first() {
            FlightInfo::new()
                .try_with_schema(first_batch.schema().as_ref())
                .map_err(|e| Status::internal(format!("Failed to encode preview schema: {e}")))?
        } else {
            FlightInfo::new()
        }
        .with_endpoint(endpoint)
        .with_descriptor(descriptor)
        .with_total_records(total_rows.cast_signed())
        .with_total_bytes(total_bytes.cast_signed());

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    // Unimplemented endpoints

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::permission_denied(
            "list_flights is disabled; obtain preview access from the controller",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::PreviewKey;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rapidbyte_engine::result::SourceTiming;
    use rapidbyte_engine::{DryRunResult, DryRunStreamResult};
    use tokio_stream::StreamExt;

    fn sign_ticket(key: &[u8], payload: &crate::ticket::TicketPayload) -> Vec<u8> {
        crate::ticket::TicketSigner::new(key).sign(payload)
    }

    fn future_expiry() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300
    }

    fn make_service() -> (PreviewFlightService, Vec<u8>, Arc<RwLock<PreviewSpool>>) {
        let key = b"test-secret-key-32-bytes-long!!!".to_vec();
        let spool = Arc::new(RwLock::new(PreviewSpool::new(
            std::time::Duration::from_secs(300),
        )));
        let service = PreviewFlightService::new(spool.clone(), &key);
        (service, key, spool)
    }

    fn users_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn orders_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap()
    }

    fn dry_run_result() -> DryRunResult {
        DryRunResult {
            streams: vec![
                DryRunStreamResult {
                    stream_name: "users".into(),
                    batches: vec![users_batch()],
                    total_rows: 3,
                    total_bytes: 12,
                },
                DryRunStreamResult {
                    stream_name: "orders".into(),
                    batches: vec![orders_batch()],
                    total_rows: 2,
                    total_bytes: 8,
                },
            ],
            source: SourceTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            duration_secs: 1.0,
        }
    }

    fn empty_stream_result() -> DryRunResult {
        DryRunResult {
            streams: vec![DryRunStreamResult {
                stream_name: "empty".into(),
                batches: vec![],
                total_rows: 0,
                total_bytes: 0,
            }],
            source: SourceTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            duration_secs: 1.0,
        }
    }

    #[tokio::test]
    async fn do_get_returns_only_requested_stream_batches() {
        let (service, key, spool) = make_service();
        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 1,
            },
            dry_run_result(),
        );

        let ticket = sign_ticket(
            &key,
            &crate::ticket::TicketPayload {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                stream_name: "users".into(),
                lease_epoch: 1,
                expires_at_unix: future_expiry(),
            },
        );

        let mut stream = service
            .do_get(Request::new(Ticket {
                ticket: ticket.into(),
            }))
            .await
            .unwrap()
            .into_inner();

        let mut flight_data = Vec::new();
        while let Some(item) = stream.next().await {
            flight_data.push(item.unwrap());
        }

        let batches = arrow_flight::utils::flight_data_to_batches(&flight_data).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().fields()[0].name(), "id");
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn do_get_returns_empty_stream_for_zero_row_preview() {
        let (service, key, spool) = make_service();
        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 1,
            },
            empty_stream_result(),
        );

        let ticket = sign_ticket(
            &key,
            &crate::ticket::TicketPayload {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                stream_name: "empty".into(),
                lease_epoch: 1,
                expires_at_unix: future_expiry(),
            },
        );

        let mut stream = service
            .do_get(Request::new(Ticket {
                ticket: ticket.into(),
            }))
            .await
            .unwrap()
            .into_inner();

        let mut flight_data = Vec::new();
        while let Some(item) = stream.next().await {
            flight_data.push(item.unwrap());
        }

        assert!(flight_data.is_empty());
    }

    #[tokio::test]
    async fn get_flight_info_returns_schema_and_ticket_for_requested_stream() {
        let (service, key, spool) = make_service();
        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 1,
            },
            dry_run_result(),
        );

        let ticket = sign_ticket(
            &key,
            &crate::ticket::TicketPayload {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                stream_name: "users".into(),
                lease_epoch: 1,
                expires_at_unix: future_expiry(),
            },
        );

        let info = service
            .get_flight_info(Request::new(FlightDescriptor::new_cmd(ticket.clone())))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(info.total_records, 3);
        assert_eq!(info.total_bytes, 12);
        assert!(!info.schema.is_empty());
        assert_eq!(info.endpoint.len(), 1);
        assert_eq!(
            info.endpoint[0].ticket.as_ref().unwrap().ticket.as_ref(),
            ticket.as_slice()
        );
        assert!(info.flight_descriptor.is_some());
    }

    #[tokio::test]
    async fn get_flight_info_supports_empty_preview_stream() {
        let (service, key, spool) = make_service();
        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 1,
            },
            empty_stream_result(),
        );

        let ticket = sign_ticket(
            &key,
            &crate::ticket::TicketPayload {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                stream_name: "empty".into(),
                lease_epoch: 1,
                expires_at_unix: future_expiry(),
            },
        );

        let info = service
            .get_flight_info(Request::new(FlightDescriptor::new_cmd(ticket.clone())))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(info.total_records, 0);
        assert_eq!(info.total_bytes, 0);
        assert_eq!(info.endpoint.len(), 1);
        assert_eq!(
            info.endpoint[0].ticket.as_ref().unwrap().ticket.as_ref(),
            ticket.as_slice()
        );
    }

    #[tokio::test]
    async fn lookup_stream_rejects_mismatched_preview_identity() {
        let (service, key, spool) = make_service();
        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 1,
            },
            dry_run_result(),
        );

        let ticket = sign_ticket(
            &key,
            &crate::ticket::TicketPayload {
                run_id: "run-2".into(),
                task_id: "task-1".into(),
                stream_name: "users".into(),
                lease_epoch: 2,
                expires_at_unix: future_expiry(),
            },
        );

        let err = service
            .do_get(Request::new(Ticket {
                ticket: ticket.into(),
            }))
            .await
            .err()
            .expect("mismatched preview identity should be rejected");

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn do_get_serves_file_backed_preview() {
        let key = b"test-secret-key-32-bytes-long!!!".to_vec();
        let spool = Arc::new(RwLock::new(PreviewSpool::with_spill_threshold(
            std::time::Duration::from_secs(300),
            1,
        )));
        let service = PreviewFlightService::new(spool.clone(), &key);

        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 1,
            },
            dry_run_result(),
        );

        let ticket = sign_ticket(
            &key,
            &crate::ticket::TicketPayload {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                stream_name: "users".into(),
                lease_epoch: 1,
                expires_at_unix: future_expiry(),
            },
        );

        let mut stream = service
            .do_get(Request::new(Ticket {
                ticket: ticket.into(),
            }))
            .await
            .unwrap()
            .into_inner();

        let mut flight_data = Vec::new();
        while let Some(item) = stream.next().await {
            flight_data.push(item.unwrap());
        }

        let batches = arrow_flight::utils::flight_data_to_batches(&flight_data).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn list_flights_is_disabled() {
        let (service, _key, spool) = make_service();
        spool.write().await.store(
            PreviewKey {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                lease_epoch: 7,
            },
            dry_run_result(),
        );

        let err = service
            .list_flights(Request::new(Criteria {
                expression: Vec::new().into(),
            }))
            .await
            .err()
            .expect("list_flights should be disabled");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

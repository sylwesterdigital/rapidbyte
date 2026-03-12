//! Arrow Flight server for dry-run preview replay.
//!
//! Validates signed tickets and streams RecordBatches from the preview spool.

use std::pin::Pin;
use std::sync::Arc;

use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::spool::PreviewSpool;
use crate::ticket::TicketVerifier;

pub struct PreviewFlightService {
    spool: Arc<RwLock<PreviewSpool>>,
    verifier: Arc<TicketVerifier>,
}

impl PreviewFlightService {
    pub fn new(spool: Arc<RwLock<PreviewSpool>>, verifier: Arc<TicketVerifier>) -> Self {
        Self { spool, verifier }
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
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

        let spool = self.spool.read().await;
        let dry_run = spool
            .get(&payload.task_id)
            .ok_or_else(|| Status::not_found("Preview not found or expired"))?;

        // Collect all batches across all streams
        let mut all_batches = Vec::new();
        for stream in &dry_run.streams {
            all_batches.extend(stream.batches.iter().cloned());
        }

        if all_batches.is_empty() {
            let stream = tokio_stream::empty();
            return Ok(Response::new(Box::pin(stream)));
        }

        let schema = all_batches[0].schema();

        // Convert schema to FlightData
        let options = IpcWriteOptions::default();
        let schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to encode schema as FlightData: {e}")))?;

        let mut flight_data_vec = vec![schema_flight_data];
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        for batch in &all_batches {
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

        let spool = self.spool.read().await;
        let dry_run = spool
            .get(&payload.task_id)
            .ok_or_else(|| Status::not_found("Preview not found or expired"))?;

        let total_records: u64 = dry_run.streams.iter().map(|s| s.total_rows).sum();
        let total_bytes: u64 = dry_run.streams.iter().map(|s| s.total_bytes).sum();

        let info = FlightInfo::new()
            .with_total_records(total_records as i64)
            .with_total_bytes(total_bytes as i64);

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
        Err(Status::unimplemented("list_flights not supported"))
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

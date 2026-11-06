# Sync Service

The Sync Service is responsible for synchronizing changes from an internal system to external systems. It consumes change events, converts them into sync commands, transforms records into provider-specific payloads, and delivers them to external systems while managing execution, retries, and rate limits.

## Overview

The service operates on the assumption that changes in the internal system are captured by an external mechanism, such as Change Data Capture (CDC). Change detection and event generation are outside the scope of this service.

Each change is represented by a `ChangeEvent`, containing the previous and current versions of a record. The service uses this information to determine what operation needs to be performed on the external system.

Tenants are assumed to have configured one or more connections to external systems. A connection represents a tenant's integration with a particular external provider and includes:
- The external system/provider to sync data to.
- Credentials required to communicate with the provider.
- Custom field mappings that override or extend the default mappings maintained by the service.

Connection management and credential configuration are outside the current scope. Credential handling is also not yet implemented.

## Processing Flow

The sync process consists of the following stages:

### 1. Change Event Processing

The service receives a `ChangeEvent` containing the previous and current versions of an internal record.

Using the tenant ID and the available connections, the service determines which external systems need to receive the change and generates the corresponding sync commands.

The operation is determined by comparing the previous and current record states:
- **Create:** A new record has been created.
- **Update:** An existing record has been modified.
- **Delete:** An existing record has been deleted.

If a tenant has `N` configured connections, a single change event can generate up to `N` sync commands, assuming the record is valid for all connections.

Connection-specific filtering (e.g., excluding certain event types from specific connections) is not currently implemented.

### 2. Transformation and Validation

Generated sync commands are passed to the transformer module.

The transformer is responsible for:
- Converting the record into the provider-compatible payload format.
- Validating the resulting payload before delivery.

Once transformation is complete, the exact payload to be sent to the external system is available.

**Mapping implementation note:** The current implementation maintains a single merged mapping combining default and tenant-specific mappings. A future refactor should separate these concerns and apply tenant-specific overrides explicitly.

### 3. Queueing

After successful transformation and validation, sync commands are added to the queue.

The queue maintains pending work for each connection, allowing the scheduler to process commands independently based on connection-specific execution constraints.

### 4. Scheduling and Execution

The scheduler manages workers associated with individual connections.

Each worker:
- Picks up pending sync commands for its connection.
- Respects the rate limit configured for that connection.
- Forwards commands to the delivery module for execution.

This allows multiple connections to be processed independently, each with its own rate limit and execution flow.

Batching: The scheduler currently processes one record at a time. Batching multiple records into a single delivery API request is not yet implemented. This can be introduced in the future based on provider-specific batching capabilities and constraints.

### 5. Delivery

The delivery module is responsible for communicating with the appropriate external provider.

Based on the provider associated with a connection, it invokes the corresponding adapter to execute the requested operation (Create, Update, or Delete).

The delivery module is also expected to own provider-specific concerns, including:
- Authentication and credential management.
- Access token generation and refresh.
- Provider API request and response handling.
- Parsing provider responses and handling errors.

Credential management and complete provider-specific authentication flows are not yet implemented.

### 6. External ID Management

External systems typically assign their own identifiers when records are created.

The service must persist the external ID returned after a successful Create operation so that subsequent Update and Delete operations can reference the correct external record.

The `IdStore` module is responsible for maintaining the mapping between internal records and their corresponding external identifiers.

### 7. Retry and Failure Handling

When delivery fails, the sync command is requeued at the front of the connection's queue.

Requeueing at the front preserves ordering and ensures that failed operations are retried before newer operations for the same connection.

The service also needs to support a Dead Letter Queue (DLQ). Once a command exhausts its configured retry attempts, it should be moved to the DLQ instead of being retried indefinitely.

DLQ handling is not yet implemented.


## Running

- Go 1.23.3 or newer should be installed
- To run the pipeline, run the end-to-end test: go test ./e2e
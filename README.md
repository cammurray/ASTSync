# Attack Simulation Training Sync Function App

The purpose of this function app is to synchronise Attack Simulation Training to Azure Table storage.

## What is stored?

The following API methods are pulled, flattened, and stored in to Azure Table Storage:
* graph.microsoft.com/beta/security/attackSimulation/simulations -> Simulations Table
* graph.microsoft.com/beta/security/attackSimulation/simulations/{id}/reports/simulationUsers -> SimulationsUsers (stores a row for every user in the simulation) and SimulationUserEvents Table (stores all events, such as click/report, etc.)
* graph.microsoft.com/beta/security/attackSimulation/payloads -> Payloads
* graph.microsoft.com/beta/security/attackSimulation/training -> Trainings

## Installation

Whilst the code here could be adjusted to suit any means, it is intended to run in an Azure Function App.

1. Create a new Azure Function App. Use the default options, and do not set up deployment at this stage unless you want to fork this repository.
2. 
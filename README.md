# GoodBarber App - Progress Analytics Backend

This repository contains the backend services that power the "My Progress," "Team Progress," and "Community Progress" charting features for the GoodBarber application. The system is designed to ingest raw user quiz data, process it, store it in a graph database, and serve aggregated analytics to the frontend.

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Features](#features)
3. [Tech Stack](#tech-stack)
4. [Setup and Installation](#setup-and-installation)
5. [API Endpoints](#api-endpoints)
6. [Frontend Integration](#frontend-integration)
7. [Database Schema](#database-schema)

## System Architecture
The system is composed of three main components that work together:

*   **Receiver Backend (`receiver_backend` - *if in a separate repo*):** A dedicated service responsible for receiving raw, unstructured log data from various app events. It performs initial validation and saves the logs for processing.
*   **API Gateway Backend (`api_gateway.py`):** The core of the system. This Flask application exposes a single, powerful API endpoint (`/records`) that the frontend consumes. It handles requests for individual, team, and community data by querying the database and performing the necessary aggregations.
*   **GoodBarber Frontend:** The frontend is not in this repository. It consists of custom HTML/JS/CSS code deployed within "Custom Code" sections in the GoodBarber app. It is responsible for fetching the logged-in user's identity, requesting data from the API Gateway, and rendering the charts using Chart.js.

## Features
- **Secure User Identification**: Robustly fetches the logged-in user's email from the GoodBarber application using a proven bridge/deep-link method.
- **Individual Progress Tracking**: Serves historical quiz score data for the logged-in user.
- **Team Progress Aggregation**: Identifies the user's team, gathers data from all team members, and calculates the daily average score for each quiz question.
- **Community Progress Aggregation**: Identifies the user's community, gathers data from all community members, and calculates the daily average score.
- **Data-Driven Chart Rendering**: The backend provides data in a clean, chart-ready format for easy consumption by the frontend.
- **Debug Logging**: Includes a `/log` endpoint to capture the raw user object from the frontend for easier debugging of the email extraction logic.

## Tech Stack
- **Backend**: Python, Flask
- **Database**: Ultipa Graph Database
- **Data Processing**: Pandas
- **Frontend**: HTML, CSS, JavaScript
- **Charting Library**: Chart.js with Luxon.js for date handling
- **Platform**: GoodBarber

## Setup and Installation

### Prerequisites
- Python 3.8+
- pip (Python package installer)
- An active Ultipa Graph Database instance

### 1. Clone the Repository
```bash
git clone https://github.com/geetanjaligit/Ultipa_backend
cd Ultipa-Backend
```

### 2. Install Dependencies
It is highly recommended to use a virtual environment.
# Create and activate a virtual environment (optional but recommended)
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

# Install required Python packages
```bash
pip install -r requirements.txt
```
(Note: You will need to create a requirements.txt file containing Flask, Flask-Cors, pandas, and ultipa-sdk)


### 3. Configure Environment Variables
The application uses environment variables for database credentials. Create a .env file in the project root and add the following:
# .env file
```bash
ULTIPA_HOST="your_ultipa_host:port"
ULTIPA_USERNAME="your_ultipa_username"
ULTIPA_PASSWORD="your_ultipa_password"
The application will automatically load these variables.
```

### 4. Run the API Gateway
To start the main backend service, run the following command:
```bash
python api_gateway.py
The service will be available at `http://localhost:5005`. For production, it is recommended to use a WSGI server like Gunicorn.
```

## API Endpoints
The primary endpoint for fetching all chart data.

### `/records`
- **Method**: `POST`
- **Description**: Retrieves progress data based on the type specified.
- **Request Body**:
```bash
{
  "type": "visitor" | "team" | "community",
  "email": "user.email@example.com"
}
```

## Example Usage
### Request for Individual Progress:
```bash
POST /records
{
  "type": "visitor",
  "email": "john.doe@example.com"
}
```
### Request for Team Progress:
```bash
POST /records
{
  "type": "team",
  "email": "jane.doe@example.com"
}
```
### Success Response (200 OK):
```bash
{
  "matched_records": [
    {
      "quiz_id": "PXRB1220",
      "question_id": "Q25547277",
      "data": [
        {
          "x": "2025-07-22T15:45:00",
          "y": 3
        },
        {
          "x": "2025-08-09T20:17:00",
          "y": 1
        }
      ]
    }
    // ... more records
  ],
  "total_records": 12
}
```

## Frontend Integration
The frontend is implemented as a "Custom Code" section within the GoodBarber CMS.
- **User Identification**: The frontend uses a robust, promise-based function getUserInfoFromGBBridge() which employs two methods to get the user object:
   - **Primary**: window.GBUser.getInfos() if available.
   - **Fallback**: A goodbarber://getuser deep-link triggered via a hidden iframe, which invokes a global callback (gbDidSuccessGetUser).
- **Data Request**: Once a valid user email is extracted, the frontend sends a POST request to the /records endpoint of this API Gateway with the appropriate type (visitor, team, or community).
- **Rendering**: The received matched_records are processed and rendered using Chart.js to create the interactive progress charts.

## Database Schema
The system relies on an Ultipa Graph Database with the following conceptual schema:
- **Nodes**:
  - `visitor`: Represents an individual app user (contains Email, visitor_id, etc.).
  - `team`: Represents a user's team (contains team_id).
  - `community`: Represents a user's community (contains community_id).
  - `record`: Represents a single quiz submission event.
- **Edges**:
  - `belongs_to_team`: Connects a visitor to their team.
  - `belongs_to_community`: Connects a visitor to their community.




# NomadIQ Navigator
End-to-end data engineering demo of the medallion model, transforming datasets through Bronze, Silver, and Gold layers with DuckDB and DuckLake as the backbone.

## Overview
Ever planned a trip to a bucket list park, only to miss hidden gems along the way? This project builds a unified analytics platform for U.S. National Parks, National Landmarks, and nearby State Parks. It consolidates scattered data sources—NPS alerts/events, the National Register of Historic Places, state park datasets, historic weather, and visitor stats—into a single dashboard. The result: richer insights to help outdoor enthusiasts find the best places and the best times to visit.

Public data is ingested, validated, and standardized into analysis-ready tables following a modern lakehouse architecture and medallion ELT pattern. The project highlights:

- End-to-end pipeline design with raw → staged → cleaned layers  
- Lightweight analytics/testing with DuckDB + DuckLake  
- Automated validation and data quality checks  
- Reusable, modular code for future extensions  
- Outputs suitable for dashboards, visualization, or cloud warehouse integration  

The repository features pipeline scripts, orchestration, and an API. 



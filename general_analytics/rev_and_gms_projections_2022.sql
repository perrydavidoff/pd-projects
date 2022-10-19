CREATE OR REPLACE EXTERNAL TABLE `etsy-data-warehouse-prod.staging.projections`
(
	date					DATE
	, gms_projections 		INT64
	, revenue_projections 	INT64
)
OPTIONS (
    uris = ["gs://etsy-dw-bucket1-prod/static_projections.csv"],
    format = "CSV",
    field_delimiter = ",",
    skip_leading_rows = 0
);
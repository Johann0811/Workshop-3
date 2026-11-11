-- =====================================================================================
-- Workshop 3 - KPIs for the dashboard
-- Every view below is read directly by the dashboard (Power BI), which queries the
-- database and never the CSV files. The script is idempotent (CREATE OR REPLACE VIEW).
-- =====================================================================================

-- KPI 1. Average prediction error ---------------------------------------------------
-- prediction_error = predicted - actual, so:
--   mean_absolute_error : typical size of the error (the headline KPI)
--   rmse                : like MAE but penalizes large mistakes more
--   mean_error_bias     : positive = the model over-predicts on average, negative = under-predicts
CREATE OR REPLACE VIEW vw_kpi_average_error AS
SELECT
    COUNT(*)                                                       AS predictions,
    ROUND(AVG(ABS(prediction_error))::numeric, 4)                  AS mean_absolute_error,
    ROUND(SQRT(AVG(prediction_error * prediction_error))::numeric, 4) AS rmse,
    ROUND(AVG(prediction_error)::numeric, 4)                       AS mean_error_bias
FROM fact_predictions;

-- KPI 2. Predictions by country -----------------------------------------------------
CREATE OR REPLACE VIEW vw_kpi_predictions_by_country AS
SELECT
    c.country_name,
    COUNT(*)                                          AS predictions,
    ROUND(AVG(f.actual_score)::numeric, 3)            AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::numeric, 3)         AS avg_predicted_score,
    ROUND(AVG(ABS(f.prediction_error))::numeric, 3)   AS mean_absolute_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name;

-- KPI 3. Predicted vs actual score (one row per prediction, for a scatter plot) -----
CREATE OR REPLACE VIEW vw_kpi_predicted_vs_actual AS
SELECT
    f.prediction_id,
    c.country_name,
    f.observation_year,
    f.actual_score,
    f.predicted_score,
    f.prediction_error,
    ABS(f.prediction_error) AS absolute_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id;

-- KPI 4a. Prediction trend over the historical observation years -------------------
CREATE OR REPLACE VIEW vw_kpi_trend_by_year AS
SELECT
    observation_year,
    COUNT(*)                                          AS predictions,
    ROUND(AVG(actual_score)::numeric, 3)              AS avg_actual_score,
    ROUND(AVG(predicted_score)::numeric, 3)           AS avg_predicted_score,
    ROUND(AVG(ABS(prediction_error))::numeric, 3)     AS mean_absolute_error
FROM fact_predictions
GROUP BY observation_year;

-- KPI 4b. Prediction trend over processing time (how the stream behaved) ----------
CREATE OR REPLACE VIEW vw_kpi_trend_by_time AS
SELECT
    date_trunc('minute', prediction_timestamp)        AS prediction_minute,
    COUNT(*)                                          AS predictions,
    ROUND(AVG(ABS(prediction_error))::numeric, 3)     AS mean_absolute_error,
    ROUND(AVG(EXTRACT(EPOCH FROM (prediction_timestamp - event_time)))::numeric, 2)
                                                      AS avg_processing_delay_seconds
FROM fact_predictions
GROUP BY date_trunc('minute', prediction_timestamp);

-- Supporting view: how many events of each kind reached the pipeline ---------------
CREATE OR REPLACE VIEW vw_event_quality AS
SELECT
    processing_status,
    COUNT(*)                                                      AS events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)            AS percentage
FROM raw_happiness_events
GROUP BY processing_status;

-- -------------------------------------------------------------------------------------
-- Handy inspection queries (not views)
-- -------------------------------------------------------------------------------------
-- Rejected events and why:
--   SELECT raw_event_id, event_id, processing_status, status_detail, raw_payload
--   FROM raw_happiness_events WHERE processing_status NOT IN ('VALID', 'RECEIVED') ORDER BY raw_event_id;
-- Trace one prediction back to the Kafka message that produced it:
--   SELECT f.prediction_id, r.kafka_partition, r.kafka_offset, r.raw_payload
--   FROM fact_predictions f JOIN raw_happiness_events r USING (raw_event_id) LIMIT 5;

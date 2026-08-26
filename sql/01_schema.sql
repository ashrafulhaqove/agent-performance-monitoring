-- ============================================================
-- 01_schema.sql
-- Star schema for the Agent Performance Monitoring System
-- Run once against ecommerce_agent_db
-- ============================================================

-- ------------------------------------------------------------
-- DIMENSION TABLES
-- ------------------------------------------------------------

CREATE TABLE dim_service_lines (
    service_line_id   INT           NOT NULL IDENTITY(1,1),
    service_line_name VARCHAR(50)   NOT NULL,
    CONSTRAINT PK_dim_service_lines PRIMARY KEY (service_line_id)
);

CREATE TABLE dim_channels (
    channel_id    INT          NOT NULL IDENTITY(1,1),
    channel_name  VARCHAR(50)  NOT NULL,
    direction     VARCHAR(10)  NOT NULL,  -- 'Inbound' | 'Outbound'
    CONSTRAINT PK_dim_channels PRIMARY KEY (channel_id)
);

CREATE TABLE dim_teams (
    team_id         INT          NOT NULL IDENTITY(1,1),
    service_line_id INT          NOT NULL,
    channel_id      INT          NOT NULL,
    team_name       VARCHAR(100) NOT NULL,
    CONSTRAINT PK_dim_teams          PRIMARY KEY (team_id),
    CONSTRAINT FK_teams_service_line FOREIGN KEY (service_line_id) REFERENCES dim_service_lines(service_line_id),
    CONSTRAINT FK_teams_channel      FOREIGN KEY (channel_id)      REFERENCES dim_channels(channel_id)
);

CREATE TABLE dim_agents (
    agent_id   INT          NOT NULL IDENTITY(1,1),
    team_id    INT          NOT NULL,
    agent_name VARCHAR(100) NOT NULL,
    hire_date  DATE         NOT NULL,
    CONSTRAINT PK_dim_agents      PRIMARY KEY (agent_id),
    CONSTRAINT FK_agents_team     FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
);

CREATE TABLE dim_date (
    date_id     INT  NOT NULL,           -- YYYYMMDD integer key
    date        DATE NOT NULL,
    day_of_week INT  NOT NULL,           -- 1=Mon … 7=Sun
    week_num    INT  NOT NULL,
    month_num   INT  NOT NULL,
    quarter     INT  NOT NULL,
    CONSTRAINT PK_dim_date PRIMARY KEY (date_id)
);

-- ------------------------------------------------------------
-- FACT TABLE
-- ------------------------------------------------------------

CREATE TABLE fact_daily_metrics (
    metric_id             INT           NOT NULL IDENTITY(1,1),
    agent_id              INT           NOT NULL,
    team_id               INT           NOT NULL,
    channel_id            INT           NOT NULL,
    date_id               INT           NOT NULL,
    -- universal KPIs
    items_handled         INT           NOT NULL,
    resolution_rate       DECIMAL(5,2)  NULL,
    csat_score            DECIMAL(4,2)  NULL,
    -- Call / Chat
    aht_seconds           INT           NULL,
    -- Call only
    first_call_resolution DECIMAL(5,2)  NULL,
    -- Chat only
    concurrent_chats_avg  DECIMAL(4,2)  NULL,
    -- Email only
    avg_response_hours    DECIMAL(6,2)  NULL,
    sla_compliance        DECIMAL(5,2)  NULL,
    -- Issue Resolution only
    avg_days_to_close     DECIMAL(6,2)  NULL,
    reopen_rate           DECIMAL(5,2)  NULL,
    CONSTRAINT PK_fact_daily_metrics  PRIMARY KEY (metric_id),
    CONSTRAINT FK_fact_agent          FOREIGN KEY (agent_id)   REFERENCES dim_agents(agent_id),
    CONSTRAINT FK_fact_team           FOREIGN KEY (team_id)    REFERENCES dim_teams(team_id),
    CONSTRAINT FK_fact_channel        FOREIGN KEY (channel_id) REFERENCES dim_channels(channel_id),
    CONSTRAINT FK_fact_date           FOREIGN KEY (date_id)    REFERENCES dim_date(date_id)
);

-- ------------------------------------------------------------
-- AGGREGATION TABLE
-- ------------------------------------------------------------

CREATE TABLE agg_team_daily (
    agg_id               INT          NOT NULL IDENTITY(1,1),
    team_id              INT          NOT NULL,
    date_id              INT          NOT NULL,
    total_items_handled  INT          NOT NULL,
    avg_aht              DECIMAL(8,2) NULL,
    avg_csat             DECIMAL(4,2) NULL,
    avg_resolution_rate  DECIMAL(5,2) NULL,
    avg_fcr              DECIMAL(5,2) NULL,   -- Call
    avg_concurrent_chats DECIMAL(4,2) NULL,   -- Chat
    avg_response_hours   DECIMAL(6,2) NULL,   -- Email
    avg_sla_compliance   DECIMAL(5,2) NULL,   -- Email / IR
    avg_days_to_close    DECIMAL(6,2) NULL,   -- IR
    avg_reopen_rate      DECIMAL(5,2) NULL,   -- IR
    CONSTRAINT PK_agg_team_daily  PRIMARY KEY (agg_id),
    CONSTRAINT UQ_agg_team_date   UNIQUE (team_id, date_id),
    CONSTRAINT FK_agg_team        FOREIGN KEY (team_id) REFERENCES dim_teams(team_id),
    CONSTRAINT FK_agg_date        FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- ------------------------------------------------------------
-- ML OUTPUT TABLES
-- ------------------------------------------------------------

CREATE TABLE fact_kpi_anomalies (
    anomaly_id    INT          NOT NULL IDENTITY(1,1),
    team_id       INT          NOT NULL,
    date_id       INT          NOT NULL,
    kpi_name      VARCHAR(50)  NOT NULL,
    anomaly_score DECIMAL(8,4) NOT NULL,
    is_anomaly    BIT          NOT NULL DEFAULT 0,
    detected_at   DATETIME     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_fact_kpi_anomalies PRIMARY KEY (anomaly_id),
    CONSTRAINT FK_anomaly_team       FOREIGN KEY (team_id) REFERENCES dim_teams(team_id),
    CONSTRAINT FK_anomaly_date       FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_agent_risk (
    risk_id         INT          NOT NULL IDENTITY(1,1),
    agent_id        INT          NOT NULL,
    scored_date     DATE         NOT NULL,
    risk_score      DECIMAL(5,4) NOT NULL,
    risk_flag       BIT          NOT NULL DEFAULT 0,
    top_risk_factor VARCHAR(100) NULL,
    scored_at       DATETIME     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_fact_agent_risk PRIMARY KEY (risk_id),
    CONSTRAINT FK_risk_agent      FOREIGN KEY (agent_id) REFERENCES dim_agents(agent_id)
);

CREATE TABLE agg_team_predictions (
    pred_id          INT          NOT NULL IDENTITY(1,1),
    team_id          INT          NOT NULL,
    prediction_date  DATE         NOT NULL,
    predicted_kpi    VARCHAR(50)  NOT NULL,
    predicted_value  DECIMAL(10,4) NOT NULL,
    lower_bound      DECIMAL(10,4) NULL,
    upper_bound      DECIMAL(10,4) NULL,
    generated_at     DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_agg_team_predictions PRIMARY KEY (pred_id),
    CONSTRAINT FK_pred_team            FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
);

-- ------------------------------------------------------------
-- PIPELINE LOG
-- ------------------------------------------------------------

CREATE TABLE pipeline_log (
    log_id        INT           NOT NULL IDENTITY(1,1),
    run_date      DATE          NOT NULL,
    pipeline_name VARCHAR(100)  NOT NULL,
    status        VARCHAR(20)   NOT NULL,  -- 'Success' | 'Failed' | 'Skipped'
    rows_loaded   INT           NULL,
    error_message VARCHAR(1000) NULL,
    duration_secs INT           NULL,
    logged_at     DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_pipeline_log PRIMARY KEY (log_id)
);

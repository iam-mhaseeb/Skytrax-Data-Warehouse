class SqlQueries:
    create_table_queries = ("""
    -- Recreate Sagging Tables
    
    DROP TABLE IF EXISTS public.stagging_airline;
    DROP TABLE IF EXISTS public.stagging_airport;
    DROP TABLE IF EXISTS public.stagging_lounge;
    DROP TABLE IF EXISTS public.stagging_seat;
    
    CREATE TABLE IF NOT EXISTS public.stagging_airline(
        airline_name                    VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        link                            VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        title                           VARCHAR(150) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author                          VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author_country                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        review_date                     DATE ENCODE RAW,
        review_content                  VARCHAR(1000) NOT NULL DEFAULT 'Unknown',
        aircraft                        VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        type_traveller                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        cabin_flown                     VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        route                           VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        overall_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_comfort_rating             FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        cabin_staff_rating              FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        food_beverages_rating           FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        inflight_entertainment_rating   FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        ground_service_rating           FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        wifi_connectivity_rating        FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        value_money_rating              FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        recommended                     BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
    );
    
    CREATE TABLE IF NOT EXISTS public.stagging_airport(
        airport_name                    VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        link                            VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        title                           VARCHAR(150) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author                          VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author_country                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        review_date                     DATE ENCODE RAW,
        review_content                  VARCHAR(1000) NOT NULL DEFAULT 'Unknown',
        experience_airport              VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        date_visit                      DATE ENCODE RAW,
        type_traveller                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        overall_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        queuing_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        terminal_cleanness_rating       FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        terminal_seating_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        terminal_signs_rating           FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        food_beverages_rating           FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_shopping_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        wifi_connectivity_rating        FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_staff_rating            FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        recommended                     BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
    );
    
    CREATE TABLE IF NOT EXISTS public.stagging_lounge(
        airline_name                    VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        link                            VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        title                           VARCHAR(150) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author                          VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author_country                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        review_date                     DATE ENCODE RAW,
        review_content                  VARCHAR(1000) NOT NULL DEFAULT 'Unknown',
        lounge_name                     VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        airport                         VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        lounge_type                     VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        date_visit                      DATE ENCODE RAW,
        type_traveller                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        overall_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        comfort_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        cleanness_rating                FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        bar_beverages_rating            FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        catering_rating                 FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        washrooms_rating                FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        wifi_connectivity_rating        FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        staff_service_rating            FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        recommended                     BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
    );
    
    CREATE TABLE IF NOT EXISTS public.stagging_seat(
        airline_name                    VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        link                            VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        title                           VARCHAR(150) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author                          VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        author_country                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        review_date                     DATE ENCODE RAW,
        review_content                  VARCHAR(1000) NOT NULL DEFAULT 'Unknown',
        aircraft                        VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        seat_layout                     VARCHAR(50) NOT NULL DEFAULT 'Unknown',
        date_flown                      DATE ENCODE RAW,
        type_traveller                  VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        overall_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_legroom_rating             FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_recline_rating             FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_width_rating               FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        aisle_space_rating              FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        viewing_tv_rating               FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        power_supply_rating             FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_storage_rating             FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        recommended                     BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
    );
    
    -- Create dimensional tables if required
    
    CREATE TABLE IF NOT EXISTS public.passengers(
        id                   INT IDENTITY(1,1) PRIMARY KEY ENCODE ZSTD,
        name                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        country              VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        type                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD
    )
    SORTKEY(name)
    DISTSTYLE AUTO;
    
    CREATE TABLE IF NOT EXISTS public.airports(
        id                   INT IDENTITY(1,1) PRIMARY KEY ENCODE ZSTD,
        name                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        link                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        experience           VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD
    )
    SORTKEY(name)
    DISTSTYLE AUTO;
    
    CREATE TABLE IF NOT EXISTS public.airlines(
        id                   INT IDENTITY(1,1) PRIMARY KEY ENCODE ZSTD,
        name                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        link                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        route                VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        cabin                VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD
    )
    SORTKEY(name)
    DISTSTYLE AUTO;
    
    CREATE TABLE IF NOT EXISTS public.aircrafts(
        id                   INT IDENTITY(1,1) PRIMARY KEY ENCODE ZSTD,
        airline_id           INT ENCODE ZSTD,
        name                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        seat_layout          VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD
    )
    SORTKEY(name)
    DISTSTYLE AUTO;
    
    CREATE TABLE IF NOT EXISTS public.lounge(
        id                   INT IDENTITY(1,1) PRIMARY KEY ENCODE ZSTD,
        airline_id           INT ENCODE ZSTD,
        name                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD,
        type                 VARCHAR(100) NOT NULL DEFAULT 'Unknown' ENCODE ZSTD
    )
    SORTKEY(name)
    DISTSTYLE AUTO;
    
    -- Create fact table if required
   CREATE TABLE IF NOT EXISTS public.fact_ratings(
        id                                    INT IDENTITY(1,1) PRIMARY KEY ENCODE ZSTD, -- Surrogate key
        -- Dimensional tables ids
        passenger_id                          INT ENCODE ZSTD,
        airport_id                            INT ENCODE ZSTD,
        airline_id                            INT ENCODE ZSTD,
        aircraft_id                           INT ENCODE ZSTD,
        lounge_id                             INT ENCODE ZSTD,
        -- Flight dates
        airport_visit_date                    DATE ENCODE RAW,
        lounge_visit_date                     DATE ENCODE RAW,
        flight_date                           DATE ENCODE RAW,
        -- Review dates
        airline_review_date                   DATE ENCODE RAW, 
        airport_review_date                   DATE ENCODE RAW,
        lounge_review_date                    DATE ENCODE RAW,
        seat_review_date                      DATE ENCODE RAW,
        -- Airline Ratings
        airline_overall_rating                FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_seat_comfort_rating           FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_cabin_staff_rating            FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_food_beverages_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_inflight_entertainment_rating FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_ground_service_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_wifi_connectivity_rating      FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_ground_service_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_value_money_rating            FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airline_recommended                   BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
        -- Airport Ratings
        airport_overall_rating                FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_queuing_rating                FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_terminal_cleanness_rating     FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_terminal_seating_rating       FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_terminal_signs_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_food_beverages_rating         FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_shopping_rating               FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_wifi_connectivity_rating      FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_staff_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        airport_recommended                   BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
        -- Lounge Ratings
        lounge_overall_rating                 FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        lounge_comfort_rating                 FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        lounge_cleanness_rating               FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        lounge_washrooms_rating               FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        lounge_wifi_connectivity_rating       FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        lounge_staff_service_rating           FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        lounge_recommended                    BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
        -- Seat Ratings
        seat_overall_rating                   FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_legroom_rating                   FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_recline_rating                   FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_width_rating                     FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_aisle_space_rating               FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_viewing_tv_rating                FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_shopping_rating                  FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_power_supply_rating              FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_storage_rating                   FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        seat_recommended                      BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD,
        -- Combined rating (airline + airport + lounge + seat)/4
        overall_rating                        FLOAT NOT NULL DEFAULT 0.0 ENCODE ZSTD,
        -- Combined recommendation average(airline + airport + lounge + seat)
        overall_recommendation                BOOL NOT NULL DEFAULT FALSE ENCODE ZSTD
    )
    SORTKEY(id)
    DISTSTYLE AUTO;
    """)


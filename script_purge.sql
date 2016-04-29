DROP FUNCTION IF EXISTS historic(int);

CREATE OR REPLACE FUNCTION historic(val int) RETURNS boolean AS $$
DECLARE
    t timestamp := CURRENT_DATE - val * INTERVAL '1 day';
    min_request_date timestamp;
    rowcount int;
    limit_tmp_req int := 100000;
    nb_tmp_req int;
BEGIN
    CREATE TEMPORARY TABLE tmp_requests (
        id bigserial, CONSTRAINT tmp_requests_pkey PRIMARY KEY (id)
    );

    CREATE TEMPORARY TABLE tmp_interpreted_parameters (
        id bigint, CONSTRAINT tmp_ip_pkey PRIMARY KEY (id)
    );

    LOOP
        SELECT MIN(request_date) INTO min_request_date FROM stat.requests;
        RAISE INFO 'Min request date = %', min_request_date;

        TRUNCATE TABLE tmp_requests;

        INSERT INTO tmp_requests (id)
        SELECT id
        FROM stat.requests
        WHERE request_date < t
        ORDER BY id
        LIMIT limit_tmp_req;

        SELECT COUNT(*) INTO nb_tmp_req FROM tmp_requests;

        RAISE INFO 'Fetched % request ids', nb_tmp_req;

        EXIT WHEN nb_tmp_req = 0;

        DELETE
        FROM stat.coverages C USING tmp_requests R
        WHERE R.id=C.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % coverages', rowcount;

        DELETE
        FROM stat.journey_request JR USING tmp_requests R
        WHERE R.id=JR.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % journey_request', rowcount;

        DELETE
        FROM stat.errors E USING tmp_requests R
        WHERE R.id=E.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % errors', rowcount;

        DELETE
        FROM stat.parameters P USING tmp_requests R
        WHERE R.id=P.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % parameters', rowcount;

        TRUNCATE TABLE tmp_interpreted_parameters;

        INSERT INTO tmp_interpreted_parameters (id)
        SELECT id
        FROM stat.interpreted_parameters
        WHERE request_id in (select id from tmp_requests);

        DELETE
        FROM stat.filter F USING tmp_interpreted_parameters I
        WHERE F.interpreted_parameter_id = I.id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % filter', rowcount;

        DELETE
        FROM stat.journey_sections JS USING tmp_requests R
        WHERE JS.request_id=R.id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % journey_sections', rowcount;

        DELETE
        FROM stat.interpreted_parameters I USING tmp_requests R
        WHERE R.id=I.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % interpreted_parameters', rowcount;

        DELETE
        FROM stat.journeys J USING tmp_requests R
        WHERE R.id=J.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % journeys', rowcount;

        DELETE
        FROM stat.info_response IR USING tmp_requests R
        WHERE R.id=IR.request_id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % info_response', rowcount;

        DELETE
        FROM stat.requests S USING tmp_requests R
        WHERE S.id = R.id;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        RAISE INFO 'Deleted % requests', rowcount;

    END LOOP;

    DROP TABLE tmp_interpreted_parameters;
    DROP TABLE tmp_requests;

    RETURN true;
END;  
$$ LANGUAGE plpgsql;

-- nb de jour a modifier
BEGIN;
select historic(:retention);
COMMIT;

VACUUM VERBOSE stat.coverages;
VACUUM VERBOSE stat.errors;
VACUUM VERBOSE stat.filter;
VACUUM VERBOSE stat.info_response;
VACUUM VERBOSE stat.interpreted_parameters;
VACUUM VERBOSE stat.journey_request;
VACUUM VERBOSE stat.journey_sections;
VACUUM VERBOSE stat.journeys;
VACUUM VERBOSE stat.parameters;
VACUUM VERBOSE stat.requests;

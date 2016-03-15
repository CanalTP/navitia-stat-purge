DROP FUNCTION IF EXISTS historic(int);

CREATE OR REPLACE FUNCTION historic(val int) RETURNS boolean AS $$
DECLARE
    t timestamp := CURRENT_DATE - val * INTERVAL '1 day';
    rowcount int;
BEGIN 
    DELETE
    FROM stat.coverages C USING stat.requests R
    WHERE R.id=C.request_id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % coverages', rowcount;

    DELETE
    FROM stat.journey_request JR USING stat.requests R
    WHERE R.id=JR.request_id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % journey_request', rowcount;

    DELETE
    FROM stat.errors E USING stat.requests R
    WHERE R.id=E.request_id
      AND R.request_date < t ;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % errors', rowcount;

    DELETE
    FROM stat.parameters P USING stat.requests R
    WHERE R.id=P.request_id
      AND R.request_date < t ;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % parameters', rowcount;

    DELETE
    FROM stat.filter F USING stat.interpreted_parameters I,
                             stat.requests R
    WHERE F.interpreted_parameter_id = I.id
      AND I.request_id=R.id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % filter', rowcount;

    DELETE
    FROM stat.journey_sections JS USING stat.requests R
    WHERE JS.request_id=R.id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % journey_sections', rowcount;

    DELETE
    FROM stat.interpreted_parameters I USING stat.requests R
    WHERE R.id=I.request_id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % interpreted_parameters', rowcount;

    DELETE
    FROM stat.journeys J USING stat.requests R
    WHERE R.id=J.request_id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % journeys', rowcount;

    DELETE
    FROM stat.info_response IR USING stat.requests R
    WHERE R.id=IR.request_id
      AND R.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % info_response', rowcount;

    DELETE
    FROM stat.requests S
    WHERE S.request_date < t;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    RAISE INFO 'Deleted % requests', rowcount;

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

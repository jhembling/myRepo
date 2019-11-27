create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.spot (
  record_type string,
  date string,
  time string,
  spotlight_id string,
  advertiser_id string,
  spotlightgroup_id string,
  GUID string,
  queryArgs string,
  browser string,
  ip_address string,
  privacy string,
  track_id string,
  user_agent string,
  spotlight_request_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

delete from airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.spot
where run_datehour = 2019070415
;

---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.spot (
  record_type,
  date,
  time,
  spotlight_id,
  advertiser_id,
  spotlightgroup_id,
  GUID,
  queryArgs,
  browser,
  ip_address,
  privacy,
  track_id,
  user_agent,
  spotlight_request_guid,
  file_source,
  load_timestamp,
  run_datehour
)

select
  record_type,
  date,
  time,
  spotlight_id,
  advertiser_id,
  spotlightgroup_id,
  GUID,
  queryArgs,
  browser,
  ip_address,
  privacy,
  track_id,
  user_agent,
  spotlight_request_guid,
  file_source,
  load_timestamp,
  run_datehour
  from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.spot
  where run_datehour = 2019070415
  ;

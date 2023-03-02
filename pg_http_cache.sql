-- [apt-get|brew] install pgxnclient
-- pgxn install pg_curl

-- Usage:
--
-- select cached_get('http://worldtimeapi.org/api/timezone/Europe/Madrid', interval '10 seconds');
-- Or with named arguments:
-- select cached_get(url => 'http://worldtimeapi.org/api/timezone/Europe/Madrid', expires_after => interval '1 second');

create extension if not exists pg_curl;
create or replace function http_get(url text) returns text
language sql as
$body$
with s as (
  select curl_easy_reset(),
  curl_easy_setopt_url(url),
  curl_easy_perform(),
  curl_easy_getinfo_data_in() 
)
select convert_from(curl_easy_getinfo_data_in, 'utf-8') from s;
$body$;

create table if not exists http_cache(
  url text primary key, data text, updated_at timestamp
);

create or replace function cached_get(url text, expires_after interval default interval '1 day') returns text
language sql as
$body$
with calculation as(
  select http_get(cached_get.url) as data
)
insert into http_cache(url, data, updated_at)
select 
  cached_get.url
, (select calculation.data from calculation)
, now()
where not exists(
  select 1
  from http_cache
  where url = cached_get.url
  and updated_at + expires_after > now()
)
on conflict(url) 
do update
set
  data = (select data from calculation)
, updated_at = now()
;
select data from http_cache where url = cached_get.url;
$body$;

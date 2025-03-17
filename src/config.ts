export const NATS_SERVERS = process.env.NATS_SERVERS || 'nats://localhost:4222'
export const NATS_TOKEN = process.env.NATS_TOKEN || ''
export const NATS_STREAM = process.env.NATS_STREAM || 'EVENTS'
export const NATS_CONSUMER =
  process.env.NATS_CONSUMER || 'online_pppoe_ip_checker'
export const NATS_SUBJECT =
  process.env.NATS_SUBJECT || 'events.online_pppoe_data_fetched'
export const INITIAL_BACKOFF_DELAY = Number(
  process.env.INITIAL_BACKOFF_DELAY || 1000,
)
export const MAX_BACKOFF_DELAY = Number(process.env.MAX_BACKOFF_DELAY || 32000)
export const MYSQL_HOST = process.env.MYSQL_HOST || 'localhost'
export const MYSQL_USER = process.env.MYSQL_USER || 'root'
export const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || ''
export const MYSQL_DATABASE = process.env.MYSQL_DATABASE || 'test'

export const METRICS_FILE = process.env.METRICS_FILE || './data/metrics.txt'
export const TEMP_METRICS_FILE = process.env.TEMP_METRICS_FILE || './data/metrics.txt.tmp'

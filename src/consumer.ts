import { AckPolicy, connect } from 'nats'
import {
  NATS_CONSUMER,
  NATS_SERVERS,
  NATS_STREAM,
  NATS_SUBJECT,
  NATS_TOKEN,
} from './config'

const nc = await connect({
  servers: NATS_SERVERS,
  token: NATS_TOKEN,
})
const js = nc.jetstream()
const jsm = await js.jetstreamManager()

await jsm.consumers.add(NATS_STREAM, {
  durable_name: NATS_CONSUMER,
  ack_policy: AckPolicy.Explicit,
  filter_subject: NATS_SUBJECT,
})

await nc.close()

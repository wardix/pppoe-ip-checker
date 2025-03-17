import { connect, type JsMsg } from 'nats'
import mysql from 'mysql2/promise'
import fs from 'fs/promises'
import path from 'path'
import {
  INITIAL_BACKOFF_DELAY,
  MAX_BACKOFF_DELAY,
  NATS_CONSUMER,
  NATS_SERVERS,
  NATS_STREAM,
  NATS_TOKEN,
  MYSQL_HOST,
  MYSQL_USER,
  MYSQL_PASSWORD,
  MYSQL_DATABASE,
  TEMP_METRICS_FILE,
  METRICS_FILE,
} from './config'
import logger from './logger'

// Define interface for database results
interface CustomerNetworkRecord {
  net: string
  csid: string
}

// Create MySQL connection pool
const pool = mysql.createPool({
  host: MYSQL_HOST,
  user: MYSQL_USER,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
})

// Collection of metrics to write
let metricsBuffer: string[] = []

// Function to add a metric to the buffer
function addMetric(metric: string): void {
  metricsBuffer.push(metric)
}

// Function to flush metrics to file
async function flushMetricsToFile(): Promise<void> {
  if (metricsBuffer.length === 0) {
    return
  }

  try {
    // Write all metrics to temporary file
    await fs.writeFile(TEMP_METRICS_FILE, metricsBuffer.join('\n') + '\n')

    // Rename temporary file to final destination (atomic operation)
    await fs.rename(TEMP_METRICS_FILE, METRICS_FILE)

    logger.info(`Wrote ${metricsBuffer.length} metrics to ${METRICS_FILE}`)

    // Clear the buffer after successful write
    metricsBuffer = []
  } catch (err) {
    logger.error('Error writing metrics to file:', err)
  }
}

// Function to clear the metrics file
async function clearMetricsFile(): Promise<void> {
  try {
    // Create an empty file at the temporary location
    await fs.writeFile(TEMP_METRICS_FILE, '')

    // Rename to the target file (atomic replacement)
    await fs.rename(TEMP_METRICS_FILE, METRICS_FILE)

    logger.info(`Cleared metrics file: ${METRICS_FILE}`)
  } catch (err) {
    logger.error('Error clearing metrics file:', err)
  }
}

async function main() {
  try {
    logger.info('Starting NATS message consumer')
    // Clear the metrics file before starting
    await clearMetricsFile()
    await consumeMessages()
  } catch (err) {
    logger.error('Fatal error:', err)
    process.exit(1)
  }
}

async function consumeMessages() {
  const nc = await connect({
    servers: NATS_SERVERS,
    token: NATS_TOKEN,
  })

  // Graceful shutdown on SIGINT and SIGTERM
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, async () => {
      logger.info(`Received ${signal}. Draining NATS connection...`)

      // Ensure any buffered metrics are written to file before shutdown
      if (metricsBuffer.length > 0) {
        logger.info(
          `Flushing ${metricsBuffer.length} pending metrics before shutdown`,
        )
        await flushMetricsToFile()
      }

      await nc.drain()
      await pool.end() // Close MySQL connections
      process.exit(0)
    })
  }

  const js = nc.jetstream()
  const consumer = await js.consumers.get(NATS_STREAM, NATS_CONSUMER)

  let backoffDelay = INITIAL_BACKOFF_DELAY
  let consecutiveErrors = 0
  const MAX_CONSECUTIVE_ERRORS = 5

  while (true) {
    try {
      const messages = await consumer.fetch({ max_messages: 1, expires: 1000 })
      let hasMessages = false

      for await (const message of messages) {
        hasMessages = true
        try {
          await processMessage(message)
          message.ack()
          // Reset backoff and error count on successful message processing
          backoffDelay = INITIAL_BACKOFF_DELAY
          consecutiveErrors = 0
        } catch (err) {
          logger.error('Error processing message:', err)
          message.nak() // Negative acknowledge to retry later
          consecutiveErrors++

          if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
            logger.error(
              `Reached ${MAX_CONSECUTIVE_ERRORS} consecutive errors, exiting...`,
            )
            await nc.drain()
            await pool.end()
            process.exit(1)
          }
        }
      }

      if (!hasMessages) {
        logger.info(`No message available. Backing off for ${backoffDelay} ms`)
        await sleep(backoffDelay)
        backoffDelay = Math.min(backoffDelay * 2, MAX_BACKOFF_DELAY)
      }
    } catch (connErr) {
      logger.error('NATS connection error:', connErr)
      await sleep(backoffDelay)
      backoffDelay = Math.min(backoffDelay * 2, MAX_BACKOFF_DELAY)
    }
  }
}

async function processMessage(message: JsMsg) {
  // Parse the message data
  const data = JSON.parse(new TextDecoder().decode(message.data))
  logger.info(`Processing message: ${message.seq}`)

  const networks: string[] = []
  const placeHolders: string[] = []
  const networkHosts = new Map<string, { host: string; iface: string }>()

  // Extract networks from the message
  for (const host in data.servers) {
    for (const { network, iface } of data.servers[host]) {
      networkHosts.set(network, { host, iface })
      networks.push(`${network}/32`)
      placeHolders.push('?')
    }
  }

  if (networks.length === 0) {
    logger.warn('No networks found in message')
    return
  }

  let connection
  try {
    // Connect to MySQL and execute a query
    connection = await pool.getConnection()

    const query = `
      SELECT cst.Network net, cst.CustServId csid
      FROM CustomerServiceTechnical cst
      LEFT JOIN CustomerServices cs ON cs.CustServId = cst.CustServId
      LEFT JOIN Customer c ON c.CustId = cs.CustId
      WHERE c.BranchId = '020'
      AND NOT (cs.ServiceId IN ('IPP'))
      AND cst.Network IN (${placeHolders.join(',')})
      ORDER BY cst.Network
    `

    // Execute the query with the network values
    const [rows] = await connection.execute(query, networks)
    const typedRows = rows as CustomerNetworkRecord[]

    // Find networks that don't exist in the query results
    const foundNetworks = new Set(typedRows.map((row) => row.net))
    const missingNetworks = networks.filter(
      (network) => !foundNetworks.has(network),
    )

    // Collect unknown PPPoE addresses metrics
    for (const e of missingNetworks) {
      const net = e.replace('/32', '')
      const { host, iface } = networkHosts.get(net) as {
        host: string
        iface: string
      }
      const cleanIface = iface.replace('<pppoe-', '').replace('>', '')
      const metric = `unknown_pppoe_address{net="${net}",host="${host}",iface="${cleanIface}"} 1`
      addMetric(metric)
    }

    // Track duplicate networks
    const networkCounts = new Map<string, number>()
    const duplicateNetworks = new Set<string>()

    for (const { net } of typedRows) {
      networkCounts.set(net, (networkCounts.get(net) || 0) + 1)
      if (networkCounts.get(net)! > 1) {
        duplicateNetworks.add(net)
      }
    }

    // Collect duplicate networks metrics
    if (duplicateNetworks.size > 0) {
      for (const net of duplicateNetworks) {
        const cleanNet = net.replace('/32', '')
        const count = networkCounts.get(net) || 0
        const metric = `duplicate_pppoe_address{net="${cleanNet}",count="${count}"} 1`
        addMetric(metric)
      }
    }

    // Flush collected metrics to file
    await flushMetricsToFile()

    return {
      processedNetworks: networks.length,
      foundRecords: typedRows.length,
      missingNetworks: missingNetworks,
      duplicateNetworks: Array.from(duplicateNetworks),
    }
  } catch (error) {
    logger.error('Database error:', error)
    throw error // Rethrow to trigger error handling in the consumer
  } finally {
    // Always release the connection back to the pool
    if (connection) connection.release()
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// Start the application
main()

import { KinesisStreamEvent } from 'aws-lambda'
import { IotCorePublisher } from './IotCorePublisher'
import { mapSourceEventToPushEvent } from './Mapper'
import { init as initMetrics, calculate as calculateMetrics } from './Metrics'
import { fromKinesisRecord } from './SourceEvent'

let publisher: IotCorePublisher // keep instances alive across executions of functions

export async function handler(data: KinesisStreamEvent) {
  ensureEnvironmentSetup()

  if (!data || !data.Records || (data && data.Records && data.Records.length === 0)) {
    console.error('handler invoked with no data or no records')
    return
  }

  createPublisher()

  const metrics = initMetrics({ batchSize: data.Records.length })

  const publications: Array<Promise<void>> = []

  for (const record of data.Records) {
    try {
      const sourceEvent = fromKinesisRecord(record)
      if (!sourceEvent) {
        console.log(`Ignoring non-regurgitator payload ${JSON.stringify(record)}`)
        metrics.ignored++
        continue
      }

      if (hasExpired(sourceEvent.timestamp)) {
        console.log(`Skip expired on type: ${sourceEvent.type}, category: ${sourceEvent.category}`)
        metrics.ignoredDueToAge++
        continue
      }

      const mappedPushEvent = mapSourceEventToPushEvent(sourceEvent)
      if (!mappedPushEvent) {
        console.log(`Skip ignored on type: ${sourceEvent.type}, category: ${sourceEvent.category}`)
        console.log(`Skip ignored payload: ${JSON.stringify(sourceEvent)}`)
        metrics.ignored++
        continue
      }

      const promise = publisher
        .send(mappedPushEvent.topic, mappedPushEvent.event)
        .then(() => {
          const event = mappedPushEvent.event
          const details = {
            topic: mappedPushEvent.topic,
            eventType: event.eventType,
            emitterId: event.emitterId,
            deliveryTime: Date.now() - event.emitterTimestamp,
          }
          metrics.published++
          metrics.deliveryTimes.push(details.deliveryTime)
          console.log(`Published ${JSON.stringify(details)}`)

          if (isDebugEnabled()) {
            console.log(`Published payload ${JSON.stringify(mappedPushEvent.event)}`)
          }
        })
        .catch((reason: Error) => {
          console.error(`Error publishing`, reason)
        })

      publications.push(promise)
    } catch (e) {
      console.error(`Error processing event`, e)
    }
  }

  await Promise.all(publications)

  console.log(`Summary metrics ${JSON.stringify(calculateMetrics(metrics))}`)
}

const ensureEnvironmentSetup = () => {
  ;[
    'PUBLISHER_ENDPOINT',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_SESSION_TOKEN',
    'AWS_REGION',
    'VERSION',
    'DEBUG_LOGGING',
  ].forEach(name => {
    const value = process.env[name]
    if (!value || value.trim().length === 0) {
      throw new Error(`Environment variable missing: ${name}`)
    }
  })
}

const createPublisher = () => {
  if (!publisher) {
    const endpoint = process.env.PUBLISHER_ENDPOINT as string
    publisher = new IotCorePublisher(endpoint)
  }
}

const hasExpired = (timestamp: string): boolean => {
  const sourceDate = new Date(timestamp)
  const maxAgeSeconds = parseInt(process.env.EVENT_EXPIRY_SECONDS || '30')
  const oldestAllowedDate = new Date(Date.now() - maxAgeSeconds * 1000)
  return sourceDate < oldestAllowedDate
}

const isDebugEnabled = (): boolean => {
  return (process.env.DEBUG_LOGGING || 'disabled').toLowerCase() === 'enabled'
}

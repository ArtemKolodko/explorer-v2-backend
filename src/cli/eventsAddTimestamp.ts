import {init as configInit} from 'src/config'
import {ContractEvent, ContractEventType, ContractType, Log, RPCBlockHarmony} from 'src/types'
import {logger} from 'src/logger'
import {storesAPI, productionAPI} from 'src/store'

const l = logger(module, `EventsAddTimestamp`)

const SHARD_ID = 0

const START_BLOCK_NUMBER = 18031163 // start block_number from logs table 7336963
const FINISH_BLOCK_NUMBER = 35997155 // end block_number from logs table 35997155
const BLOCKS_BATCH_SIZE = 1000

const SUCCESS_SLEEP_TIMEOUT = 100
const FAIL_SLEEP_TIMEOUT = 60000

const apiStore = storesAPI[SHARD_ID] // events database store
const productionApiStore = productionAPI[SHARD_ID]

const sleep = (timeout: number) => new Promise((resolve) => setTimeout(resolve, timeout))

const getContractEvents = async (blockFrom: number, blockTo: number) => {
  const res = await apiStore.address.query(
    `
    select * from contract_events
    where block_number >= $1 and block_number <= $2
  `,
    [blockFrom, blockTo]
  )
  return res
}

const filterEvents = (events: Array<{from: string}>) => {
  const eventsMap: Record<string, any> = {}
  events.forEach(({from}) => {
    if (eventsMap[from]) {
      eventsMap[from] += 1
    } else {
      eventsMap[from] = 1
    }
  })
  const sortable = []
  const sendersToDelete: string[] = []
  // eslint-disable-next-line guard-for-in
  for (const address in eventsMap) {
    if (eventsMap[address] >= 100) {
      delete eventsMap[address]
      sendersToDelete.push(address)
    }
  }

  for (const address in eventsMap) {
    sortable.push([address, eventsMap[address]])
  }

  sortable.sort(function (a: any, b: any) {
    return b[1] - a[1]
  })
  // console.log('sortable', sortable.slice(0, 3))
  // console.log('events', events.length)
  const filtered = events.filter((event) => !sendersToDelete.includes(event.from))
  // console.log('filtered', filtered.length)
  // console.log('sendersToDelete', sendersToDelete.length)
  return filtered
}

const getBlocksFromProductionsDB = async (blocksList: string[]) => {
  const res = await productionApiStore.block.query(`select * from blocks where number = ANY ($1)`, [
    blocksList,
  ])
  return res
}

const updateContractEventsTimestamp = async (blocks: RPCBlockHarmony[]) => {
  return await Promise.all(
    blocks.map(async (block) => {
      await apiStore.address.query(
        `
    update contract_events set timestamp = $1 where block_number = $2
  `,
        [block.timestamp, +block.number]
      )
    })
  )
}

const updateContractEventsTimestampBatch = async (blocks: RPCBlockHarmony[]) => {
  const values = blocks
    .map(({number, timestamp}) => {
      const unixTimestamp = new Date(timestamp).valueOf()
      return `(${number}, to_timestamp(${unixTimestamp} / 1000.0)::timestamp)`
    })
    .join(',')

  const query = `
    update contract_events
    set timestamp = e2.timestamp
    from (
        values ${values}
    ) as e2(block_number, timestamp)
    where e2.block_number = contract_events.block_number;
  `
  await apiStore.address.query(query, [])
}

const startMigration = async () => {
  let isStopped = false
  let blocksFrom = START_BLOCK_NUMBER
  let blocksTo = START_BLOCK_NUMBER + BLOCKS_BATCH_SIZE - 1

  const increaseBlocksNumber = () => {
    blocksFrom += BLOCKS_BATCH_SIZE
    blocksTo = blocksFrom + BLOCKS_BATCH_SIZE - 1
  }

  while (!isStopped) {
    try {
      l.info(`Working on blocks ${blocksFrom} - ${blocksTo}`)

      console.time('get_contract_events')
      const originalEvents = await getContractEvents(blocksFrom, blocksTo)
      const events = filterEvents(originalEvents)
      const uniqueBlockNumbers = events.reduce((acc: any, currentItem: any) => {
        const {block_number} = currentItem
        if (!acc.includes(block_number)) {
          acc.push(block_number)
        }
        return acc
      }, [])

      // process.exit(1)

      l.info(
        `Received ${events.length} events (total: ${originalEvents.length}), ${uniqueBlockNumbers.length} uniqueBlockNumbers for blocks ${blocksFrom} - ${blocksTo} (${BLOCKS_BATCH_SIZE} blocks)`
      )
      console.timeEnd('get_contract_events')

      if (blocksTo >= FINISH_BLOCK_NUMBER) {
        isStopped = true
      }

      if (events.length === 0) {
        increaseBlocksNumber()
        // await sleep(100)
        continue
      }

      console.time('get_production_blocks')
      const productionDbBlocks = await getBlocksFromProductionsDB(uniqueBlockNumbers)
      console.timeEnd('get_production_blocks')

      console.time('update_contract_events')
      // await updateContractEventsTimestamp(productionDbBlocks)
      await updateContractEventsTimestampBatch(productionDbBlocks)
      console.timeEnd('update_contract_events')

      // process.exit(1)

      await sleep(SUCCESS_SLEEP_TIMEOUT)
      // await sleep(4000)

      increaseBlocksNumber()
    } catch (e) {
      l.error(`Events migration error: ${e.message}, sleep`)
      process.exit(1)
      // await sleep(FAIL_SLEEP_TIMEOUT)
    }
  }
  l.info(`Events migration completed on block ${blocksFrom}, exit`)
  process.exit(1)
}

;(async () => {
  await configInit()
  l.info(`Events add timestamp starting...`)
  startMigration()
})()

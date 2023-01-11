import {config} from 'src/config'
import * as RPCClient from 'src/indexer/rpc/client'
import {urls, RPCUrls} from 'src/indexer/rpc/RPCUrls'
import {ShardID, Log, ContractEventType, ContractEvent} from 'src/types/blockchain'

import {logger} from 'src/logger'
import LoggerModule from 'zerg/dist/LoggerModule'
import {stores} from 'src/store'
import {logTime} from 'src/utils/logTime'
import {PostgresStorage} from 'src/store/postgres'
import {ABIFactory} from 'src/indexer/indexer/contracts/erc20/ABI'
import {normalizeAddress} from 'src/utils/normalizeAddress'

const approximateBlockMintingTime = 2000
const blockRange = 10
const maxBatchCount = 100

const range = (num: number) => Array(num).fill(0)
const transferSignature = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

// todo make a part of blockindexer
export class LogIndexer {
  readonly shardID: ShardID
  private l: LoggerModule
  private batchCount = 10
  readonly store: PostgresStorage

  private erc20Map: Record<string, string> = {}

  constructor(shardID: ShardID) {
    if (shardID !== 0 && shardID !== 1) {
      throw new Error('Only shards #0 and #1 are currently supported')
    }

    this.l = logger(module, `shard${shardID}`)
    this.shardID = shardID
    this.store = stores[shardID]
    this.l.info('Created')
  }

  increaseBatchCount = () => {
    this.batchCount = Math.min(Math.ceil(this.batchCount * 1.1), maxBatchCount)
    this.l.debug(`Batch increased to ${this.batchCount}`)
  }

  decreaseBatchCount = () => {
    this.batchCount = Math.max(~~(this.batchCount * 0.9), 1)
    this.l.debug(`Batch decreased to ${this.batchCount}`)
  }

  private getErc20Map = async () => {
    const contracts = await stores[this.shardID].erc20.getAllERC20()
    return contracts.reduce((acc: any, value) => {
      acc[value.address] = value.symbol
      return acc
    }, {})
  }

  private writeContractEventsToDbBatch = async (events: ContractEvent[], batchSize = 1000) => {
    const store = stores[this.shardID]
    for (let i = 0; i < events.length; i += batchSize) {
      await store.contract.addContractEventsBatch(events.slice(i, i + batchSize))
    }
  }

  loop = async () => {
    try {
      const shardID = this.shardID
      const store = this.store
      const batchTime = logTime()
      const failedCountBefore = RPCUrls.getFailedCount(shardID)
      const latestSyncedBlock = await store.indexer.getLastIndexedLogsBlockNumber()
      // todo check in full sync
      const startBlock = latestSyncedBlock > 0 ? latestSyncedBlock + 1 : 0
      const latestBlockchainBlock = (await RPCClient.getBlockByNumber(shardID, 'latest', false))
        .number

      const erc20Map =
        Object.keys(this.erc20Map).length === 0 ? await this.getErc20Map() : this.erc20Map

      const addLogs = (logs: Log[]) => {
        return Promise.all(
          logs.map(async (log) => {
            await store.log.addLog(log)
            return log
          })
        )
      }

      const {getEntryByName, decodeLog} = ABIFactory(store.shardID)

      const parseContractEventFromLog = (log: Log) => {
        const [topic0, ...topics] = log.topics
        const decodedLog = decodeLog(ContractEventType.Transfer, log.data, topics)
        const tokenAddress = normalizeAddress(log.address) as string
        const from = normalizeAddress(decodedLog.from) as string
        const to = normalizeAddress(decodedLog.to) as string
        const value =
          typeof decodedLog.value !== 'undefined' ? BigInt(decodedLog.value).toString() : undefined
        return {
          address: tokenAddress,
          from,
          to,
          value,
          blockNumber: parseInt(log.blockNumber, 16).toString(),
          logIndex: parseInt(log.logIndex, 16).toString(),
          transactionIndex: parseInt(log.transactionIndex, 16).toString(),
          transactionHash: log.transactionHash,
          transactionType: 'erc20',
          eventType: ContractEventType.Transfer,
        } as ContractEvent
      }

      const addContractEvents = async (logs: Log[]) => {
        const transferLogs = logs.filter(
          ({topics, address}) => topics.includes(transferSignature) && erc20Map[address]
        )
        const contractEvents = transferLogs.map(parseContractEventFromLog)
        await this.writeContractEventsToDbBatch(contractEvents)
        return contractEvents
      }

      const res = await Promise.all(
        range(this.batchCount).map(async (_, i) => {
          const from = startBlock + i * blockRange
          const to = Math.min(from + blockRange - 1, latestBlockchainBlock)

          if (from > latestBlockchainBlock) {
            return Promise.resolve(null)
          }

          const logs = await RPCClient.getLogs(shardID, from, to, undefined, [transferSignature])
          const events = await addContractEvents(logs)
          return {
            logs,
            events,
          }
        })
      )

      const logs = res.filter((l) => l) as Array<{logs: Log[]; events: ContractEvent[]}>
      const logsLength = logs.reduce((a, b) => a + b.logs.length, 0)
      const eventsLength = logs.reduce((a, b) => a + b.events.length, 0)
      const failedCount = RPCUrls.getFailedCount(shardID) - failedCountBefore
      const syncedToBlock = Math.min(
        latestBlockchainBlock,
        startBlock + blockRange * this.batchCount
      )

      this.l.info(
        `Processed [${startBlock},${syncedToBlock}] ${Math.max(
          syncedToBlock - startBlock,
          1
        )} blocks. ${logsLength} log entries, ${eventsLength} events entries. Done in ${batchTime()}.`
      )

      await store.indexer.setLastIndexedLogsBlockNumber(syncedToBlock)

      if (logs.length === this.batchCount) {
        if (failedCount > 0) {
          this.decreaseBatchCount()
        } else {
          this.increaseBatchCount()
        }

        process.nextTick(this.loop)
      } else {
        this.decreaseBatchCount()
        setTimeout(this.loop, approximateBlockMintingTime)
      }
    } catch (err) {
      this.l.warn(`Batch failed. Retrying in ${approximateBlockMintingTime}ms`, err.message || err)
      this.decreaseBatchCount()
      setTimeout(this.loop, approximateBlockMintingTime)
    }
  }
}

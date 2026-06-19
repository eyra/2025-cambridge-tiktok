import { Bridge } from "./types/modules";
import ReactEngine from "./visualization/react/engine";
import ReactFactory from "./visualization/react/factory";
import { PageFactory } from "./visualization/react/factories/base";
import CommandRouter from './command_router'
import WorkerProcessingEngine from "./processing/worker_engine";
import { LogForwarder, LogLevel, WindowLogSource } from "./logging";

// CAMBRIDGE-FORK: This file diverges from upstream Feldspar — the constructor
// accepts a `locale` and forwards it to `WorkerProcessingEngine` so the Python
// donation script can localize DataFrame column headers. See port/script.py
// `process(data)`, port/main.py `start(data)`, py_worker.js (event.data.data),
// worker_engine.ts, and script_host_component.tsx for the matching
// divergences. When syncing feldspar/develop, keep all five.
export default class Assembly {
  processingEngine: WorkerProcessingEngine;
  visualizationEngine: ReactEngine;
  router: CommandRouter
  logForwarder: LogForwarder
  windowLogSource: WindowLogSource

  constructor(worker: Worker, bridge: Bridge, locale: string = "en", factories: PageFactory[] = [], logLevel: LogLevel = 'warn') {
    const sessionId = String(Date.now())
    const visualizationFactory = new ReactFactory(factories);
    this.visualizationEngine = new ReactEngine(visualizationFactory);
    this.router = new CommandRouter(bridge, this.visualizationEngine)
    this.logForwarder = new LogForwarder((entries) => bridge.sendLogs(entries), logLevel)
    this.windowLogSource = new WindowLogSource(this.logForwarder)
    this.processingEngine = new WorkerProcessingEngine(sessionId, locale, worker, this.router, this.logForwarder)
  }
}

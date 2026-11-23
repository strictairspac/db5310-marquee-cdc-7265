/** @typedef {{id:number, title:string, payload:object, author:string}} BusinessCase */
/** @typedef {{id:number, case_id:number, status:'queued'|'running'|'complete'|'failed'}} Run */
/** @typedef {{stream:string, sequence:number, signs:Array<{sign_id:string, content:string}>}} InventoryLedger */
/** @typedef {{id:number, run_id:number, digest:string, report:{summary:string, metrics:object, signable:boolean, blockers:string[], sections:Array, inventory:{entries:Array}}}} Evidence */

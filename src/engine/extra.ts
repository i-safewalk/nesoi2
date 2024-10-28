import { TaskMethod } from "../builders/method"

export class Extra {

    static async run(extra: TaskMethod<any,any,any,any>, methodArgs: any, event: any, stepData: any) {
        
        const promise = extra(methodArgs)
        const res = await Promise.resolve(promise)
        const resEvent: any = {}
        const resStepData: any = {}
        for (const key in res) {
            const value = res[key]
            if (key.startsWith('_step_')) {
                resStepData[key.replace('_step_', '')] = value
            } else {
                resEvent[key] = value
            }
        }
        Object.assign(event, resEvent);
        Object.assign(stepData, resStepData);
    }

}
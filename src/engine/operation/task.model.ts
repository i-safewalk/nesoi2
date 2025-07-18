import { ResourceObj } from "../data/model"
import { TaskGraph } from "./graph.model"

export type TaskAction =
    'request'
    | 'schedule'
    | 'advance'
    | 'execute'
    | 'comment'
    | 'graph'
    | 'cancel'
    | 'interrupt'
    | 'update'
    | 'backward'
    | 'skip'

export type TaskState =
    'requested'
    | 'done'
    | 'canceled'
    | 'interrupted'

export type TaskStep = {
    from_state: 'void' | TaskState
    to_state: TaskState
    user?: {
        id: number | string,
        name: string
    }
    timestamp?: string
    skipped?: boolean  // true if step was skipped
    duration?: number | undefined
    runtime?: number | undefined
    data?: Record<string, any> | undefined
}

export interface TaskModel {
    id: number
    type: string
    state: TaskState
    input: Record<string, any>
    output: {
        data: Record<string, any>
        steps: TaskStep[]
    }
    graph: TaskGraph
    created_by: number|string
    updated_by: number|string
    created_at: any
    updated_at: any
}

export interface TaskLogModel<Event> {
    id: number
    task_id: number
    task_type: string
    action: TaskAction
    state: TaskState
    message: string
    event?: Event
    timestamp: string
    user: number | string
    created_by: number|string
    updated_by: number|string
    created_at: any
    updated_at: any
}

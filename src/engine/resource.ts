import { Bucket } from "./data/bucket";
import { ResourceObj } from "./data/model";
import { NesoiError } from "../error";
import { ViewObj } from "./resource/resource_obj";
import { StateMachine } from "./resource/state_machine";
import { View } from "./resource/view";
import { CreateSchema } from "./schema";
import { NesoiClient } from "../client";

type Obj = Record<string, any>

export class Resource<
    Model extends ResourceObj,
    Events,
    Views extends Record<string, View<any>>
> {

    protected name: string
    protected alias: string
    protected views: Views

    protected createSchema: CreateSchema
    protected bucket: Bucket<any>
    public machine: StateMachine<Model, Events>

    constructor(builder: any) {
        this.name = builder.name
        this.alias = builder._alias
        this.createSchema = builder._create
        this.bucket = builder.bucket

        this.views = {} as Views
        for (const v in builder._views) {
            (this.views as any)[v] = new View(builder._views[v]);
        }

        this.machine = new StateMachine(
            builder,
            this.bucket
        )
    }

    async readOne<V extends keyof Views>(
        id: Model['id'],
        view: V|'raw' = 'raw'
    ) {
        // 1. Read from Data Source
        const promise = this.bucket.get({} as any, id);
        const model = await Promise.resolve(promise)
        if (!model) {
            throw NesoiError.Resource.NotFound(this.name, id)
        }
        // 2. If raw view, build a Obj from the model
        if (view === 'raw') {
            return this.build<Model>(model, model)
        }
        // 3. If not, build a Obj from the view result
        const viewSchema = this.views[view || 'default']
        const parsedView = await viewSchema.parse(model)
        return this.build<TViewObj<Views[V]>>(model, parsedView)
    }

    async readAll<V extends keyof Views>(
        id: Model['id'],
        view: V|'raw' = 'raw'
    ) {
        // 1. Read from Data Source
        const promise = this.bucket.index({} as any);
        const models = await Promise.resolve(promise)
        // 2. If raw view, build a list of Objs from the model list
        if (view === 'raw') {
            return Promise.all(models.map(model =>
                this.build<Model>(model, model)
            ))
        }
        // 3. If not, build a list of Objs from the view results
        const v = this.views[view || 'default']
        const parsedViews = await Promise.all(
            models.map(model => v.parse(model))
        )
        return Promise.all(parsedViews.map((view, i) => 
            this.build<Views[V]>(models[i], view)
        ))
    }

    async create(
        client: NesoiClient<any, any>,
        event: Events extends { create: any } ? Events['create'] : never
    ) {
        // 1. Parse event
        const parsedEvent = await this.createSchema.event.parse(client, event)

        // 2. Run event through method to build obj
        const promise = this.createSchema.method({ client, event: parsedEvent, obj: undefined })
        const obj = await Promise.resolve(promise) as ResourceObj

        // 3. Set crud meta
        obj.state = this.machine.getInitialState() || 'void'
        obj.created_by = client.user.id
        obj.updated_by = client.user.id


        return this.bucket.put(client, obj)
    }

    async update(
        client: NesoiClient<any, any>,
        id: Model['id'],
        event: Events extends { [key: string]: any } ? Events[keyof Events] : never
    ) {
        // 1. Parse event
        const parsedEvent = await this.createSchema.event.parse(client, event);
        
        // 2. Get existing object
        const existingObj = await this.bucket.get(client, id);
        if (!existingObj) {
            throw NesoiError.Resource.NotFound(this.name, id);
        }
        
        // 3. Run event through method to update obj
        const updatedObj = await this.createSchema.method({ client, event: parsedEvent, obj: existingObj });
        
        // 4. Set crud meta
        updatedObj.updated_by = client.user.id;
        updatedObj.updated_at = new Date().toISOString();
        
        // 5. Save updated object
        return this.bucket.put(client, updatedObj);
    }

    build<T>(model: Obj, view: Obj) {
        return new ViewObj(
            this as any,
            model as any,
            view as any
        ) as any as ResourceObj & T
    }

}

type TViewObj<
    V extends View<any>,
    R = ReturnType<V['parse']>
> = R extends { then: any } ? Awaited<R> : R
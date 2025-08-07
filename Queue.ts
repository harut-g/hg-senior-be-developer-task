import {Message} from "./Database";

export class Queue {
    private messages: Map<string, Message>
    private keysMeta: Map

    constructor() {
        this.messages = new Map()
        this.keysMeta = new Map()
    }

    SetKeyMeta(key: string, processing: boolean, workerId: number): void {
        this.keysMeta.set(key, new Map())
        this.keysMeta.get(key)
            .set('processing', processing)
            .set('workerId', workerId)
    }

    Enqueue = (message: Message) => {
        this.messages.set(message.id, message)
    }

    Dequeue = (workerId: number): Message | undefined => {
        for (const message of this.messages.values()) {
            const key = message?.key
            const processingWorkerId = this.keysMeta.get(key)?.get('workerId')

            if (!this.keysMeta.get(key)?.get('processing')) {
                this.SetKeyMeta(key, true, workerId)
                return message
            } else if(processingWorkerId === workerId) {
                return message
            }
        }
    }

    Confirm = (workerId: number, messageId: string) => {
        const key = this.messages.get(messageId)?.key
        const keyMeta = this.keysMeta.get(key)
        const isKeyProcessing = keyMeta.get('processing')
        const isCorrectWorker = keyMeta.get('workerId') === workerId

        if (isKeyProcessing && isCorrectWorker) {
            this.messages.delete(messageId);
        }

    }

    Size = () => {
        return this.messages.size
    }
}


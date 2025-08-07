import {Message} from "./Database";

export class Queue {
    private messages: Map<string, Message>
    private messagesMetaByKey: Map

    constructor() {
        this.messages = new Map()
        this.messagesMetaByKey = new Map()
    }

    Enqueue = (message: Message) => {
        this.messages.set(message.id, message)
    }

    Dequeue = (workerId: number): Message | undefined => {
        for (const message of this.messages.values()) {
            const key = message?.key
            const processingWorkerId = this.messagesMetaByKey.get(key)?.get('workerId')
            if (!this.messagesMetaByKey.get(key)?.get('processing')) {
                this.messagesMetaByKey.set(key, new Map())
                const messageMetaByKey = this.messagesMetaByKey.get(key)
                messageMetaByKey.set('processing', true)
                messageMetaByKey.set('workerId', workerId)
                return message
            } else if(processingWorkerId === workerId) {
                return message
            }
        }
    }

    Confirm = (workerId: number, messageId: string) => {
        const message = this.messages.get(messageId)
        const messageMetaByKey = this.messagesMetaByKey.get(message?.key)
        const isKeyProcessing = messageMetaByKey.get('processing')
        const isCorrectWorker = messageMetaByKey.get('workerId') === workerId

        if (isKeyProcessing && isCorrectWorker) {
            this.messages.delete(messageId);
        }

    }

    Size = () => {
        return this.messages.size
    }
}


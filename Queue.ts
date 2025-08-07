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
            const id = message?.id
            if (!this.messagesMetaByKey.get(key)?.get('processing')) {
                this.messagesMetaByKey.set(key, new Map())
                const messageMetaByKey = this.messagesMetaByKey.get(key)
                messageMetaByKey.set('processing', true)
                messageMetaByKey.set('workerId', workerId)
                messageMetaByKey.set('id', id)
                return message
            }
        }
    }

    Confirm = (workerId: number, messageId: string) => {
        const message = this.messages.get(messageId)
        const messageMetaByKey = this.messagesMetaByKey.get(message?.key)
        const id = messageMetaByKey.get('id')
        const isKeyProcessing = messageMetaByKey.get('processing')
        const isCorrectWorker = messageMetaByKey.get('workerId') === workerId

        if (isKeyProcessing && isCorrectWorker) {
            if (!id) {
                 this.messagesMetaByKey.delete(message?.key)
            } else {
                messageMetaByKey.delete('id')
                this.messagesMetaByKey.delete(message?.key)
            }
            this.messages.delete(messageId);
        }


    }

    Size = () => {
        return this.messages.size
    }
}


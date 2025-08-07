import {Message} from "./Database";

export class Queue {
    private messages: Map<string, Message>
    private messagesMeta: { [key: string]: any }

    constructor() {
        this.messages = new Map()
        this.messagesMeta = {}
    }

    Enqueue = (message: Message) => {
        this.messages.set(message.id, message)
    }

    Dequeue = (workerId: number): Message | undefined => {
        for (const message of this.messages.values()) {
            if (!this.messagesMeta[message?.key]?.processing) {
                this.messagesMeta[message?.key] = {}
                this.messagesMeta[message?.key].processing = true;
                this.messagesMeta[message?.key].workerId = workerId;
                this.messagesMeta[message?.key].ids = [message?.id];
                return message
            }
        }
    }

    Confirm = (workerId: number, messageId: string) => {
        const message = this.messages.get(messageId)
        const messageMetaByKey = this.messagesMeta[message?.key]
        const isKeyProcessing = messageMetaByKey?.processing
        const isCorrectWorker = messageMetaByKey.workerId === workerId

        if (isKeyProcessing && isCorrectWorker) {
            if (!messageMetaByKey?.ids.length) {
                delete this.messagesMeta[message?.key]
            } else {
                messageMetaByKey.ids = messageMetaByKey.ids.filter((id: string) => id !== message.id)
                if (!messageMetaByKey?.ids.length) {
                    delete this.messagesMeta[message?.key]
                }
            }
            this.messages.delete(messageId);
        }


    }

    Size = () => {
        return this.messages.size
    }
}


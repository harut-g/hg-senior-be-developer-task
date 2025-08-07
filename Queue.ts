import {Message} from "./Database";

export class Queue {
    private messages: Message[]
    private messagesMap: {}
    private messagesMeta: { [key: string]: any }

    constructor() {
        this.messages = []
        this.messagesMap = {}
        this.messagesMeta = {}
    }

    Enqueue = (message: Message) => {
        this.messages.push(message)
        this.messagesMap[message.id] = message
    }

    Dequeue = (workerId: number): Message | undefined => {
        for (let i = 0; i < this.messages.length; i++) {
            const message = this.messages[i]
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
        const message = this.messagesMap[messageId]
        const messageMetaByKey = this.messagesMeta[message?.key]
        const isKeyProcessing = messageMetaByKey?.processing
        const isCorrectWorker = messageMetaByKey.workerId === workerId

        if (isKeyProcessing && isCorrectWorker) {
            const index = this.messages.findIndex(item => item.id === messageId);
            if (!messageMetaByKey?.ids.length) {
                delete this.messagesMeta[message?.key]
                delete this.messagesMap[message?.id]
            } else {
                messageMetaByKey.ids = messageMetaByKey.ids.filter(id => id !== message.id)
                if (!messageMetaByKey?.ids.length) {
                    delete this.messagesMeta[message?.key]
                    delete this.messagesMap[message?.id]
                }
            }
            this.messages.splice(index, 1);
        }


    }

    Size = () => {
        return this.messages.length
    }
}


import { Message, BaseMessageStream } from "@hediet/json-rpc";

/**
 * Wraps a write and read stream for JSON-RPC message communication.
 *
 * Inlined from @hediet/json-rpc-streams to avoid dependency on
 * an unpublished package fetched via external URL (gitpkg.now.sh).
 */
export class NodeJsMessageStream extends BaseMessageStream {
  constructor(
    private readonly writeStream: NodeJS.WritableStream,
    private readonly readStream: NodeJS.ReadableStream,
  ) {
    super();

    let closed = false;

    readStream.on("data", (chunk: any) => {
      const str = chunk.toString("utf8");
      const parts = str.trim().split("\n");
      for (const p of parts) {
        const obj = JSON.parse(p) as Message;
        this.onMessage(obj);
      }
    });

    readStream.on("close", () => {
      if (!closed) {
        closed = true;
        this.onConnectionClosed();
      }
    });

    writeStream.on("close", () => {
      if (!closed) {
        closed = true;
        this.onConnectionClosed();
      }
    });
  }

  public close(): void {
    this.writeStream.end();
  }

  public dispose(): void {
    this.close();
  }

  public write(message: Message): Promise<void> {
    const str = JSON.stringify(message);

    return new Promise((res, rej) => {
      this.writeStream.write(str + "\n", (err) => {
        if (err) {
          rej(err);
        } else {
          res();
        }
      });
    });
  }

  public toString(): string {
    return `${this.id}@stream`;
  }
}

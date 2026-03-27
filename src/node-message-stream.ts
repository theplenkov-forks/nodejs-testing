import { Message, BaseMessageStream } from "@hediet/json-rpc";
import split from "split2";

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

    const closedOnce = () => {
      if (!closed) {
        closed = true;
        this.onConnectionClosed();
      }
    };

    readStream.pipe(split()).on("data", (line: string) => {
      if (!line) {
        return;
      }
      try {
        const obj = JSON.parse(line) as Message;
        this.onMessage(obj);
      } catch {
        // ignore malformed lines
      }
    });

    readStream.on("close", closedOnce);
    readStream.on("error", closedOnce);

    writeStream.on("close", closedOnce);
    writeStream.on("error", closedOnce);
  }

  public close(): void {
    this.writeStream.end(() => {
      if (typeof (this.readStream as any).destroy === "function") {
        (this.readStream as any).destroy();
      }
      if (typeof (this.writeStream as any).destroy === "function") {
        (this.writeStream as any).destroy();
      }
    });
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

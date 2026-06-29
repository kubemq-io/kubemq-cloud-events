declare module 'eventsource' {
  class EventSource {
    constructor(url: string, eventSourceInitDict?: { headers?: Record<string, string> });
    onmessage: ((event: MessageEvent) => void) | null;
    onerror: ((event: Event) => void) | null;
    onopen: ((event: Event) => void) | null;
    readonly readyState: number;
    readonly url: string;
    close(): void;
    addEventListener(type: string, listener: (event: MessageEvent) => void): void;
    removeEventListener(type: string, listener: (event: MessageEvent) => void): void;
    static readonly CONNECTING: number;
    static readonly OPEN: number;
    static readonly CLOSED: number;
  }
  export = EventSource;
}

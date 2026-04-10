export class ImageCache {
  private cache = new Map<string, HTMLImageElement | null>();
  private loading = new Map<string, Promise<HTMLImageElement | null>>();

  get(url?: string): HTMLImageElement | null {
    if (!url) return null;
    return this.cache.get(url) ?? null;
  }

  load(url?: string): Promise<HTMLImageElement | null> {
    if (!url) return Promise.resolve(null);
    if (this.cache.has(url)) {
      return Promise.resolve(this.cache.get(url) ?? null);
    }
    const existing = this.loading.get(url);
    if (existing) return existing;

    const promise = new Promise<HTMLImageElement | null>((resolve) => {
      const img = new Image();
      img.crossOrigin = "anonymous";
      img.onload = () => {
        this.cache.set(url, img);
        this.loading.delete(url);
        resolve(img);
      };
      img.onerror = () => {
        this.cache.set(url, null);
        this.loading.delete(url);
        resolve(null);
      };
      img.src = url;
    });

    this.loading.set(url, promise);
    return promise;
  }

  prefetch(urls: string[]): void {
    for (const url of urls) {
      void this.load(url);
    }
  }
}

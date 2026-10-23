package mutationgateway

import "context"

// The custody facade keeps its historical API and exact error identity while
// the image protocol lives in the dependency-light mutationclient package.
func (c *Client) SealImage(ctx context.Context, seal ImageSeal) (ImageSealReceipt, error) {
    return c.image.SealImage(ctx, seal)
}
func (c *Client) ImageSealStatus(ctx context.Context, seal ImageSeal) (ImageSealStatus, error) {
    return c.image.ImageSealStatus(ctx, seal)
}
func (c *Client) ImageStatus(ctx context.Context, ref ImageRef) (ImageView, error) {
    return c.image.ImageStatus(ctx, ref)
}
func (c *Client) RebindImageReader(ctx context.Context, change ImageReaderChange) (ImageReaderReceipt, error) {
    return c.image.RebindImageReader(ctx, change)
}
func (c *Client) ImageReadback(ctx context.Context, pinned ImageView, index int64) ([]byte, error) {
    return c.image.ImageReadback(ctx, pinned, index)
}

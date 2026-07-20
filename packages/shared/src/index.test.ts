import { describe, expect, it } from 'vitest';

describe('shared package exports', () => {
  it('exports artifact schemas', async () => {
    const mod = await import('./schemas/artifact');
    expect(mod.ArtifactSchema).toBeDefined();
    expect(mod.ArtifactTypeSchema).toBeDefined();
  });

  it('exports orderRecipe schemas', async () => {
    const mod = await import('./schemas/orderRecipe');
    expect(mod.OrderRecipeSchema).toBeDefined();
    expect(mod.OrderSideSchema).toBeDefined();
  });

  it('exports portfolio schemas', async () => {
    const mod = await import('./schemas/portfolio');
    expect(mod.PortfolioSnapshotSchema).toBeDefined();
  });
});

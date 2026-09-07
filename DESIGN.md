# Build Prompt: BNB Chain Cinematic Homepage Clone

Build a single-page landing site that closely recreates `https://www.bnbchain.org/en` as a dark, cinematic BNB Chain homepage. The page uses the current Next.js app scaffold, real extracted BNB assets, layered 3D-feeling SVG backgrounds, rotating headline animation, liquid/dark card depth, and responsive layouts matching the original desktop and mobile compositions.

## Tech Stack

- **Framework:** Next.js 16 App Router, React 19, TypeScript strict.
- **Styling:** Tailwind CSS v4 through `src/app/globals.css`.
- **UI primitives:** Existing shadcn/ui setup may be used where appropriate, but the homepage itself is mostly custom section components to preserve pixel fidelity.
- **Icons:** Use extracted SVG icons in `src/components/icons.tsx`; use local image/SVG assets from `public/images`.
- **Images:** Use `next/image` for local raster/SVG assets. Use `unoptimized` for extracted SVG/PNG clone assets when exact rendering is more important than optimization transforms.

## Page Structure

The page is assembled in `src/app/page.tsx` in this exact order:

1. `Header`
2. `HeroSection`
3. `TrendsSection`
4. `ProgramsSection`
5. `ProjectsSection`
6. `ToolsSection`
7. `FooterSection`

The whole page uses a continuous dark BNB background system, anchored around `#14151A` and `#181A1E`. Avoid light section backgrounds except for small CTA pills/buttons that are white or BNB yellow.

## Fonts

Use Google Fonts via `next/font/google` in `src/app/layout.tsx`:

- `Space Grotesk`: weights `400`, `500`, `600`, `700`; primary UI/body/headline font.
- `Zen Dots`: weight `400`; available as a display accent/fallback because the original font stack exposes it in computed styles.

CSS theme font tokens:

```css
--font-sans: "Space Grotesk", -apple-system, ".SFNSText-Regular", "San Francisco",
  BlinkMacSystemFont, ".PingFang-SC-Regular", "Microsoft YaHei", "Segoe UI",
  "Helvetica Neue", Helvetica, Arial, sans-serif;
--font-heading: "Space Grotesk", sans-serif;
--font-display: "Zen Dots", "Space Grotesk", sans-serif;
```

Typography must not use viewport-based scaling. Use fixed responsive breakpoints such as `text-[36px] md:text-[56px] lg:text-[72px]`.

## Color Tokens

Core colors:

- Page black: `#14151A`
- Card dark: `#181A1E`
- Section/card secondary: `#1E2026`
- Divider: `#373943`
- BNB yellow: `#FFE900`
- BNB brand yellow: `#F0B90B`
- White text: `#FFFFFF`
- Muted text: `#8C8F9B`
- Soft text: `#C4C5CB`
- White surface: `#F7F7F8`

Use these colors consistently. Do not introduce green, blue, purple, or generic gradients outside the extracted BNB accent glows.

## Global CSS Animation Utilities

All custom animation utilities live in `src/app/globals.css` outside Tailwind-only generated classes so they always emit.

### Hero 3D Background Layers

Two absolute background layers use the extracted SVG assets:

- Left layer: `/images/new-home/left.svg`
- Right layer: `/images/new-home/right.svg`

Desktop sizing:

```css
background-size: 761px 468px;
```

Positions:

```css
left layer: calc(50% - 760px) center;
right layer: calc(50% + 760px) center;
```

Motion:

- `.bnb-hero-left`: slow `translate3d` + small `rotateX`.
- `.bnb-hero-right`: slow `translate3d` + small `rotateY`.
- Animation duration: 9-10s, `ease-in-out`, infinite.
- Opacity: about `0.62`.

### Rotating Hero Word Animation

The original site rotates the first headline line through:

1. `AI-First.`
2. `Low Latency.`
3. `Low Gas Fee.`
4. `MEV-Protected.`

Implementation:

- Use a `.bnb-word-stage` wrapper with `position: relative`, fixed height `1.28em`, and `perspective: 900px`.
- Each `.bnb-word` is `position: absolute; inset: 0; display: flex; justify-content: center`.
- Color: `#FFE900`.
- Animate `opacity` and `rotateX/translateY` with matrix3d-like CSS transforms.
- Duration: `8s`.
- Delays: 0s, 2s, 4s, 6s.
- Only one word should be visible at a time.

Animation keyframe behavior:

- Start: opacity 0, rotated forward/down.
- Active range: opacity 1, transform none.
- Exit: opacity 0, rotate upward/back.

### Card Depth and Glow

Use `.bnb-card-3d` on interactive cards/media blocks:

- `transform-style: preserve-3d`
- Transition `transform`, `box-shadow`, and `border-color` in `0.18s ease`
- Hover transform: `translateY(-4px) scale(1.01) rotateX(1.5deg)`
- Hover shadow: `0 26px 72px rgb(0 0 0 / 0.42)`

Use `.bnb-glow` on dark cards:

- Pseudo-element radial glows:
  - Yellow glow: `rgb(255 233 0 / 0.22)`
  - Green-ish BNB accent glow: `rgb(24 220 126 / 0.16)`
- Default opacity 0.
- Hover opacity 1.
- Do not let glow cover text; pseudo-element must be pointer-events none and sit beneath readable content.

### Reduced Motion

Respect `prefers-reduced-motion: reduce`:

- Disable all hero float, word-cycle, raise-in, and pulse animations.
- Keep the first rotating word visible without transform.

## Header

File: `src/components/Header.tsx`

Desktop:

- Height: `64px`.
- Position: sticky top, high z-index.
- Background: `#14151A`.
- Horizontal padding: `px-4 md:px-6`.
- Inner max width: `1320px`.
- Brand left:
  - BNB cube mark from `BnbLogoIcon`.
  - Code-native text `BNB CHAIN` in `#F0B90B`, bold, `18px`.
- Center nav, desktop only:
  - Links: `Chains`, `Build`, `Explore`, `Accelerate`, `Connect`.
  - Font size: `13px`.
  - Font weight: `600`.
  - Text color: `#C4C5CB`, hover white.
  - Dropdown chevron uses extracted `ChevronDownIcon`.
- Dropdown panels:
  - Absolute below nav item.
  - Width: about `256px`.
  - Background: `#1E2026`.
  - Border: white at 10% opacity.
  - Radius: `12px`.
  - Link hover: `rgba(255,255,255,0.10)`.
- Right controls:
  - `Ask AI +`: rounded full, border `#F0B90B`, yellow text.
  - `Contact Us`: white pill/rounded rectangle, black text.
- Mobile:
  - Show hamburger icon.
  - Dropdown opens below header with grouped child links.

## Hero Section

File: `src/components/HeroSection.tsx`

Layout:

- Full-width dark section.
- Background: `#14151A`.
- Padding: `px-4 py-16 md:px-6 md:py-20`.
- Content max width: `1200px`.
- Layer order:
  1. Base dark background.
  2. Linear fade to dark.
  3. Left 3D SVG background layer.
  4. Right 3D SVG background layer.
  5. Foreground content.

Headline:

- Centered.
- Font: Space Grotesk.
- Weight: `700`.
- Size: `36px md:56px lg:72px`.
- Desktop line-height: `92px`.
- First line is the rotating yellow word stage.
- Second line: `All In One BNB.` in white.

Hero CTA:

- Text: `Contact Us`.
- Background: `#FFE900`.
- Text: `#181A1E`.
- Height: `56px`.
- Radius: `8px`.
- Padding: horizontal `24px`.
- Includes extracted `ArrowRightIcon`.

Ask AI search pill:

- Width: max `440px`.
- Height: `48px`, `56px` on md+.
- Background: `#F7F7F8`.
- Rounded full.
- Prompt text: `How do I get BNB?`
- Inner button:
  - Text: `Ask AI +`
  - Background: `#14151A`
  - Text: `#FFE900`
  - Pulse ring animation.

Stats row:

- Five stats:
  - `2.981M` / `Daily Active User`
  - `$4.875B` / `Total Value Locked`
  - `$1.811B` / `Trading Volume`
  - `$0.003517` / `Gas Fee`
  - `650ms` / `Finality Time`
- Grid:
  - Mobile: 2 columns.
  - Tablet: 3 columns.
  - Desktop: 5 columns.
- Value: `24px`, semibold, white.
- Label: `13-14px`, semibold, muted.
- Desktop dividers between items: `#373943`.

## Trends Section

File: `src/components/TrendsSection.tsx`

Background:

- `#14151A`.
- Padding: `60px` vertical.

Heading:

- Centered row with fire emoji inside yellow circle.
- Text: `Build the Next Big Trend on Chain`.
- Size: `24px md:32px`.
- Weight: `700`.
- White.

Cards:

- Three cards in 1/2/3 columns responsive grid.
- Outer border gradient with dark/yellow edges.
- Radius: `24px`.
- Shadow: `0 24px 64px rgba(0,0,0,0.48)`.
- Inner card background: `#181A1E`.
- Padding: `31px`.
- Use `.bnb-card-3d` and `.bnb-glow`.

Card content:

- `Stablecoin` with small yellow `12.2M MAU` tag.
- `Real World Assets`.
- `Memecoin`.
- Title size: `24px`, bold, white.
- Body: `16px`, line-height `24px`, color `#C4C5CB`.

Trust logos:

- Label: `Trusted by Leading Exchanges, Wallets, and Builders`.
- Color: `#8C8F9B`.
- Logos use extracted files in `/images/new-home/trust`.
- Layout wraps with `gap-x-14 gap-y-6`.

## Programs Section

File: `src/components/ProgramsSection.tsx`

Background:

- `#14151A`.
- Padding: `60px` vertical.

Heading:

- `Proof That Builders Grow Here`.
- Desktop: `48px / 56px`.
- Mobile: `32px / 40px`.
- White, bold, centered.

Builder mosaic:

- Desktop: 3 columns, gap `16px`.
- Column 1:
  - Image `b1.png`, height `144px`, radius `28px`.
  - Stat card `92,768`, height `146px`.
  - Stat card `232`, height `194px`.
- Column 2:
  - Stat card `4,828`, height `194px`.
  - Image `b2.png`, height `306px`.
- Column 3:
  - Image `b3.png`, min-height `516px`.
- All media/stat blocks use `.bnb-card-3d`.
- Stat card:
  - Background: `#181A1E`.
  - Border: `#1E2026`.
  - Radius: `28px`.
  - Padding: `32px`.
  - Number: `48px`, weight `500`, white.
  - Label: `16px`, `#C4C5CB`.

Available programs:

- Header row with `Available Programs` and desktop `View All`.
- Three dark program cards.
- Card background: `#1E2026`.
- Radius: `12px`.
- Padding: `16px`.
- Tags are small yellow-on-dark rounded pills.
- Include `CalendarIcon` + `Always Available`.

## Projects Section

File: `src/components/ProjectsSection.tsx`

Background:

- `#14151A`.
- Padding: `60px` vertical.

Heading:

- `Real Web3 Projects You Can Learn From`.
- Desktop size: `48px / 56px`.
- Mobile size: `32px / 40px`.
- White.
- `View All` button on right with white border.

Cards:

- Grid: 1 column mobile, 2 columns tablet, 4 columns desktop.
- Card background: `#1E2026`.
- Radius: `12px`.
- Padding: `16px`.
- Min height: `310px`.
- Use `.bnb-card-3d` and `.bnb-glow`.
- Image:
  - Aspect ratio `16 / 9`.
  - Rounded `8px`.
  - Use local extracted project images.
  - `loading="eager"` and `unoptimized` to avoid blank full-page screenshot captures.
- Title: `16px`, bold, white, line-clamped.
- Description: `13px`, line-height `20px`, muted, line-clamped.
- Tags: rounded full `#373943`, text `#C4C5CB`.

## Tools Section

File: `src/components/ToolsSection.tsx`

Background:

- `#14151A`.
- Overlay extracted `/images/new-home/bg.png` at low opacity (`0.20`).

Layout:

- Desktop grid: `420px 1fr`.
- Left column: heading, subheading, CTA.
- Right column: 2-column tool card grid.
- Mobile: single column.

Heading:

- `Everything You Need to Start`.
- Desktop: `48px / 56px`.
- Mobile: `32px / 40px`.
- White.

CTA:

- Text: `Explore More Tools`.
- Background: `#F7F7F8`.
- Text: `#181A1E`.
- Radius: `8px`.
- Padding: `24px 14px`.

Tool cards:

- Outer gradient border with BNB yellow transparency.
- Radius: `12px`.
- Inner card background: `#181A1E`.
- Height: `176px`.
- Padding: `32px`.
- Icons: extracted SVGs from `/images/tools`, inverted to white.
- Use `.bnb-card-3d` and `.bnb-glow`.

## Footer

File: `src/components/FooterSection.tsx`

Background:

- `#181A1E`.
- Border top: white at 6% opacity.
- Padding: top `32px`, bottom `24px`.

Top row:

- BNB icon + `BNB CHAIN` code-native text in `#F0B90B`.
- Social links represented as compact one-letter marks.

Link columns:

- Five columns:
  - Chains
  - Use BNB Chain
  - Build
  - Participate
  - About
- Column title: white, `14px`, bold.
- Link text: `14px`, semibold, `#8C8F9B`, hover white.

Bottom row:

- `© 2026 BNB Chain. All rights reserved.`
- Right links/buttons: `Cookies`, `English`.

## Responsive Requirements

Desktop `1440px`:

- Header is full nav.
- Hero stats are one row with vertical dividers.
- Trends cards are 3 columns.
- Programs mosaic is 3 columns.
- Projects are 4 columns.
- Tools are split text/card layout.
- Footer is multi-column.

Tablet `768px`:

- Header switches toward mobile if needed.
- Card grids become 2 columns where natural.
- Maintain dark visual continuity.

Mobile `390px`:

- Header uses hamburger.
- Hero headline remains centered and should not overflow horizontally.
- Rotating word stage must fit within `100vw - 32px`.
- Stats wrap into 2 columns, with final item naturally aligning.
- Programs mosaic stacks vertically.
- Projects stack one card per row.
- Footer columns stack.

## Interactions and Motion

Required:

- Header dropdown hover on desktop.
- Mobile menu toggle.
- Rotating hero keyword animation.
- Hero left/right SVG float motion.
- Ask AI pulse.
- Card hover lift/glow/depth.
- Project/tool/trend cards should feel interactive but not over-animated.

Not required:

- Real AI chat.
- Real backend data.
- Real cookie consent clone.
- Real live metric fetching.

## Assets

Use only local extracted assets unless explicitly adding new ones:

- Hero/background:
  - `/images/new-home/left.svg`
  - `/images/new-home/right.svg`
  - `/images/new-home/bg.png`
- Builder mosaic:
  - `/images/new-home/builder/b1.png`
  - `/images/new-home/builder/b2.png`
  - `/images/new-home/builder/b3.png`
- Trust logos:
  - `/images/new-home/trust/*.svg`
- Project cards:
  - `/images/projects/mcp.png`
  - `/images/projects/langchain.png`
  - `/images/projects/eip7702.png`
  - `/images/projects/pancakeswap.png`
- Tool icons:
  - `/images/tools/faucet.svg`
  - `/images/tools/bridge.svg`
  - `/images/tools/networks.svg`

## Verification

Before considering the clone complete:

1. Run `npm run check`.
2. Confirm lint has no errors. A known research-script warning about an unused variable is acceptable only if unchanged and unrelated.
3. Capture desktop screenshot at `1440x900`.
4. Capture mobile screenshot at `390x844`.
5. Confirm:
   - Header logo is correct size.
   - Rotating hero word shows only one yellow word at a time.
   - Hero background layers are visible and moving subtly.
   - Project images load.
   - Mobile text does not overflow.
   - Dark section continuity matches the original.
   - Hover cards have lift/glow effects.

Known intentional gap:

- The live site's cookie consent banner and floating third-party AI/cookie widgets are not part of the core clone unless explicitly requested.
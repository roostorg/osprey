import { afterEach, describe, expect, it } from '@rstest/core';
import { cleanup, render, screen } from '@testing-library/react';

import Markdown from './Markdown';

afterEach(cleanup);

describe('Markdown', () => {
  it('renders markdown formatting', () => {
    render(<Markdown>{'**bold** text'}</Markdown>);
    expect(screen.getByText('bold').tagName.toLowerCase()).toBe('strong');
  });

  it('does not inject raw HTML from model output', () => {
    const { container } = render(<Markdown>{'<img src=x onerror="alert(1)"> plain text'}</Markdown>);
    expect(container.querySelector('img')).toBeNull();
    expect(container.textContent).toContain('plain text');
  });
});

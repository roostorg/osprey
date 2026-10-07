'use strict';

// Share dropdown in the nav bar, similar to the theme picker
(function share() {
  const toggleButton = document.getElementById('share-toggle');
  const popup = document.getElementById('share-list');
  if (!toggleButton || !popup) {
    return;
  }

  const alternate = document.querySelector(
    'link[rel="alternate"][type="text/markdown"]',
  );
  const mdUrl = alternate ? alternate.href : null;

  if (!mdUrl) {
    popup
      .querySelectorAll('[data-share="markdown"], [data-share="copy"]')
      .forEach((item) => item.closest('li').remove());
  }

  function items() {
    return Array.from(popup.querySelectorAll('button'));
  }

  // Fetch on open so the click handler doesn't waste time fetching
  let markdown = null;
  function prefetch() {
    if (mdUrl && !markdown) {
      markdown = fetch(mdUrl).then((response) => {
        if (!response.ok) {
          throw new Error(response.status);
        }
        return response.text();
      });
    }
  }

  function show() {
    popup.style.display = 'block';
    toggleButton.setAttribute('aria-expanded', true);
    prefetch();
    const first = items()[0];
    if (first) {
      first.focus();
    }
  }

  function hide() {
    popup.style.display = 'none';
    toggleButton.setAttribute('aria-expanded', false);
    toggleButton.focus();
  }

  // Prevent a rapid second click
  function flash(item, message) {
    if (!item.dataset.label) {
      item.dataset.label = item.textContent;
    }
    clearTimeout(item.timer);
    item.textContent = message;
    item.timer = setTimeout(() => {
      item.textContent = item.dataset.label;
    }, 1500);
  }

  async function shareOrCopy(url) {
    if (navigator.share) {
      try {
        await navigator.share({ title: document.title, url });
        return null;
      } catch (e) {
        // Share sheet dismissed; don't fall back to copying
        if (e.name === 'AbortError') {
          return null;
        }
      }
    }
    await navigator.clipboard.writeText(url);
    return 'Copied!';
  }

  async function act(item) {
    switch (item.dataset.share) {
      case 'link':
        return shareOrCopy(window.location.href);
      case 'markdown':
        return shareOrCopy(mdUrl);
      case 'copy':
        await navigator.clipboard.writeText(await markdown);
        return 'Copied!';
    }
    return null;
  }

  popup.addEventListener('click', (e) => {
    const item = e.target.closest('button[data-share]');
    if (!item) {
      return;
    }
    act(item)
      .then((message) => {
        if (message) {
          flash(item, message);
        } else {
          hide();
        }
      })
      .catch(() => {
        markdown = null;
        flash(item, 'Couldn’t copy');
      });
  });

  toggleButton.addEventListener('click', () => {
    if (popup.style.display === 'block') {
      hide();
    } else {
      show();
    }
  });

  popup.addEventListener('focusout', (e) => {
    // e.relatedTarget is null in Safari and Firefox on macOS, hence the click
    // handler below as well (see rust-lang/mdBook#628)
    if (
      !!e.relatedTarget &&
      !toggleButton.contains(e.relatedTarget) &&
      !popup.contains(e.relatedTarget)
    ) {
      hide();
    }
  });

  document.addEventListener('click', (e) => {
    if (
      popup.style.display === 'block' &&
      !toggleButton.contains(e.target) &&
      !popup.contains(e.target)
    ) {
      hide();
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) {
      return;
    }
    if (!popup.contains(e.target)) {
      return;
    }

    const buttons = items();
    const index = buttons.indexOf(document.activeElement);

    switch (e.key) {
      case 'Escape':
        e.preventDefault();
        hide();
        break;
      case 'ArrowUp':
        e.preventDefault();
        if (index > 0) {
          buttons[index - 1].focus();
        }
        break;
      case 'ArrowDown':
        e.preventDefault();
        if (index !== -1 && index < buttons.length - 1) {
          buttons[index + 1].focus();
        }
        break;
      case 'Home':
        e.preventDefault();
        buttons[0].focus();
        break;
      case 'End':
        e.preventDefault();
        buttons[buttons.length - 1].focus();
        break;
    }
  });
})();

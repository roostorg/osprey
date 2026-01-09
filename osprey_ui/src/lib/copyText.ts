// the clipboard api is not available when using a non-secure host (aside from localhost)
// this can be pretty common for some setups that are running i.e. on tailscale, so ideally
// we don't want to break things when a user tries to copy text somewhere. here we use a
// fallback when either that api is not available or `secureContext` is false
// see: https://developer.mozilla.org/en-US/docs/Web/API/Navigator/clipboard
export async function copyText(text: string): Promise<void> {
  if (navigator.clipboard && window.isSecureContext) {
    return navigator.clipboard.writeText(text);
  } else {
    // create a textarea that gets appended to the body, then focus+copy text
    const body = document.body;
    const textarea = document.createElement('textarea');
    textarea.setAttribute('aria-hidden', 'true');
    body.appendChild(textarea);
    textarea.value = text;
    textarea.focus();
    textarea.select();

    // NOTE: execCommand is actually deprecated, so this might break in future browser versions
    // unfortunately. we'll log an error in the event that things do fail.
    // see: https://developer.mozilla.org/en-US/docs/Web/API/Document/execCommand
    try {
      document.execCommand('copy');
    } catch (err) {
      console.error(`copyText: fallback failed: ${err}`);
      // still throw the error so that calling code can optionally alert the user that the copy
      // failed
      throw err;
    } finally {
      textarea.remove();
    }
  }
}

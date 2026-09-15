const windowButtons = document.querySelectorAll('[data-window-button]');
const windowPanels = document.querySelectorAll('[data-window]');

windowButtons.forEach(button => {
  button.addEventListener('click', () => {
    windowButtons.forEach(candidate => {
      candidate.setAttribute('aria-pressed', String(candidate === button));
    });
    windowPanels.forEach(panel => {
      panel.hidden = panel.dataset.window !== button.dataset.windowButton;
    });
  });
});

function showEvidence() {
  const element = document.getElementById(location.hash.slice(1));
  if (element && element.matches('details.message-detail')) {
    element.open = true;
  }
}

window.addEventListener('hashchange', showEvidence);
showEvidence();

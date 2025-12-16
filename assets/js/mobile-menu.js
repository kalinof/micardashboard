// assets/js/mobile-menu.js
document.addEventListener('DOMContentLoaded', function () {
  const btn = document.getElementById('mobile-menu-button');
  const menu = document.getElementById('mobile-menu');
  const menuPanel = menu ? menu.querySelector('div') : null;
  const overlay = document.getElementById('mobile-menu-overlay');
  const hamburger = document.getElementById('hamburger-icon');
  const closeIcon = document.getElementById('close-icon');

  if (!btn || !menu || !menuPanel || !overlay) return;

  // open menu
  function openMenu() {
    btn.setAttribute('aria-expanded', 'true');
    overlay.classList.remove('hidden');
    menu.classList.remove('hidden');

    // animate in
    requestAnimationFrame(() => {
      menuPanel.classList.remove('opacity-0', 'translate-y-6');
      menuPanel.classList.add('opacity-100', 'translate-y-0');
      hamburger.classList.add('hidden');
      closeIcon.classList.remove('hidden');
    });

    // prevent page scroll optionally
    document.documentElement.classList.add('overflow-hidden');
    document.body.classList.add('overflow-hidden');

    // focus first menu item
    const first = menuPanel.querySelector('[role="menuitem"]');
    if (first) first.focus();
  }

  // close menu
  function closeMenu() {
    btn.setAttribute('aria-expanded', 'false');

    // animate out
    menuPanel.classList.remove('opacity-100', 'translate-y-0');
    menuPanel.classList.add('opacity-0', 'translate-y-6');

    hamburger.classList.remove('hidden');
    closeIcon.classList.add('hidden');

    // restore scroll
    document.documentElement.classList.remove('overflow-hidden');
    document.body.classList.remove('overflow-hidden');

    // wait for animation, then hide container and overlay
    setTimeout(() => {
      menu.classList.add('hidden');
      overlay.classList.add('hidden');
    }, 220);

    // return focus to toggle
    btn.focus();
  }

  btn.addEventListener('click', function () {
    const expanded = btn.getAttribute('aria-expanded') === 'true';
    if (expanded) closeMenu();
    else openMenu();
  });

  // Click outside to close
  overlay.addEventListener('click', closeMenu);

  // Close on Escape
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      const expanded = btn.getAttribute('aria-expanded') === 'true';
      if (expanded) closeMenu();
    }
  });

  // Close when selecting a menu item and optionally trigger tab switches
  menuPanel.querySelectorAll('[role="menuitem"]').forEach((item) => {
    item.addEventListener('click', (event) => {
      const tabTarget = item.getAttribute('data-tab-target');
      if (tabTarget && typeof window.switchTab === 'function') {
        event.preventDefault();
        window.switchTab(tabTarget);
      }
      closeMenu();
    });
  });
});

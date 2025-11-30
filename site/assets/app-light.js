// Theme manager (light default): load saved theme or fall back to light; toggle on click; keep header padding
(function(){
  var key = 'theme';
  var html = document.documentElement;
  var saved = null;
  try { saved = localStorage.getItem(key); } catch(e){}
  var initial = (saved === 'light' || saved === 'dark') ? saved : 'light';
  html.setAttribute('data-theme', initial);
  var btn = document.getElementById('themeToggle');
  if (btn) {
    btn.style.display = '';
    btn.disabled = false;
    btn.addEventListener('click', function(){
      var current = html.getAttribute('data-theme') || 'light';
      var next = current === 'dark' ? 'light' : 'dark';
      html.setAttribute('data-theme', next);
      try { localStorage.setItem(key, next); } catch(e){}
    });
  }
  var header = document.querySelector('.site-header');
  function apply(){ if(header){ document.documentElement.style.setProperty('--header-h', header.offsetHeight+'px'); } }
  if (document.readyState === 'complete' || document.readyState === 'interactive') apply();
  window.addEventListener('load', apply);
  window.addEventListener('resize', apply);
})();

// Nav active state + header opacity on scroll (same as dark)
(function(){
  var header = document.querySelector('.site-header');
  var links = Array.prototype.slice.call(document.querySelectorAll('.nav a[href^="#"]'));
  var sections = links.map(function(a){ try { return document.querySelector(a.getAttribute('href')); } catch(e){ return null; } });
  function setActive(idx){ links.forEach(function(a,i){ a.classList.toggle('active', i===idx); }); }

  if ('IntersectionObserver' in window){
    var io = new IntersectionObserver(function(entries){
      entries.forEach(function(entry){
        var idx = sections.indexOf(entry.target);
        if (entry.isIntersecting) setActive(idx);
      });
    }, { rootMargin: '-40% 0px -50% 0px', threshold: 0.01 });
    sections.forEach(function(s){ if (s) io.observe(s); });
  }

  function onScroll(){ if (header) header.classList.toggle('scrolled', window.scrollY > 10); }
  onScroll();
  window.addEventListener('scroll', onScroll, { passive:true });
  links.forEach(function(a,i){ a.addEventListener('click', function(){ setActive(i); }); });
})();

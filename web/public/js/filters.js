document.addEventListener('DOMContentLoaded', function() {
  var form = document.querySelector('form[method="GET"]');
  if (!form) return;

  form.addEventListener('submit', function(e) {
    var inputs = form.querySelectorAll('input, select');
    inputs.forEach(function(input) {
      if (!input.value) input.removeAttribute('name');
    });
  });
});

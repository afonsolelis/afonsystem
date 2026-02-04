document.addEventListener('DOMContentLoaded', function() {
  var form = document.querySelector('form[method="GET"]');
  if (!form) return;

  form.addEventListener('submit', function(e) {
    var inputs = form.querySelectorAll('input, select');
    inputs.forEach(function(input) {
      if (!input.value) input.removeAttribute('name');
    });
  });

  var btnExport = document.getElementById('btn-export-json');
  if (btnExport) {
    btnExport.addEventListener('click', function() {
      var params = new URLSearchParams(window.location.search).toString();
      var url = '/api/export' + (params ? '?' + params : '');

      fetch(url)
        .then(function(res) { return res.json(); })
        .then(function(data) {
          var blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
          var a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = 'metricas_' + new Date().toISOString().slice(0, 10) + '.json';
          a.click();
          URL.revokeObjectURL(a.href);
        })
        .catch(function() {
          alert('Erro ao exportar as métricas.');
        });
    });
  }
});

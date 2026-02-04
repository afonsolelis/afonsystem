var COLORS = [
  '#0d6efd', '#6610f2', '#6f42c1', '#d63384', '#dc3545',
  '#fd7e14', '#ffc107', '#198754', '#20c997', '#0dcaf0'
];

function renderInlineChart(canvasId, type, data) {
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;
  data.datasets.forEach(function(ds, i) {
    if (!ds.backgroundColor) ds.backgroundColor = COLORS[i % COLORS.length];
    if (!ds.borderColor && type === 'line') ds.borderColor = COLORS[i % COLORS.length];
  });
  new Chart(canvas, {
    type: type,
    data: data,
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom' } }
    }
  });
}

document.addEventListener('DOMContentLoaded', function() {
  var params = window.location.search;
  var canvases = document.querySelectorAll('canvas[data-endpoint]');

  canvases.forEach(function(canvas) {
    var endpoint = canvas.dataset.endpoint;
    var chartType = canvas.dataset.chart || 'bar';
    var url = '/api/' + endpoint + params;

    fetch(url)
      .then(function(res) { return res.json(); })
      .then(function(data) {
        if (!data.labels || data.labels.length === 0) {
          canvas.parentElement.innerHTML = '<div class="no-data">Sem dados</div>';
          return;
        }
        data.datasets.forEach(function(ds, i) {
          if (!ds.backgroundColor) ds.backgroundColor = COLORS[i % COLORS.length];
          if (!ds.borderColor && chartType === 'line') ds.borderColor = COLORS[i % COLORS.length];
        });
        new Chart(canvas, {
          type: chartType,
          data: data,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
          }
        });
      })
      .catch(function() {
        canvas.parentElement.innerHTML = '<div class="no-data">Erro ao carregar</div>';
      });
  });
});

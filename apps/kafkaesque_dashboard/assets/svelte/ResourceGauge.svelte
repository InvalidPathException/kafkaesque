<script>
  import { onMount, onDestroy } from 'svelte';
  import Chart from 'chart.js/auto';
  
  export let label = "CPU";
  export let value = 0;
  export let max = 100;
  export let unit = "%";
  
  let canvas;
  let chart;
  
  function getColor(percentage) {
    if (percentage < 60) return '#52c41a';
    if (percentage < 80) return '#faad14';
    return '#f5222d';
  }
  
  function updateChart(val, maximum) {
    if (!chart) return;
    
    const percentage = (val / maximum) * 100;
    const color = getColor(percentage);
    
    chart.data.datasets[0].data = [val, maximum - val];
    chart.data.datasets[0].backgroundColor = [color, '#1a1a1a'];
    chart.data.datasets[0].borderColor = [color, '#262626'];
    chart.options.plugins.tooltip.callbacks.label = function() {
      return `${val}${unit} / ${maximum}${unit}`;
    };
    chart.update('none');
  }
  
  onMount(() => {
    const ctx = canvas.getContext('2d');
    
    chart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        datasets: [{
          data: [value, max - value],
          backgroundColor: ['#52c41a', '#1a1a1a'],
          borderColor: ['#52c41a', '#262626'],
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '70%',
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            enabled: false
          }
        }
      }
    });
    
    updateChart(value, max);
  });
  
  onDestroy(() => {
    if (chart) {
      chart.destroy();
    }
  });
  
  $: if (chart) {
    updateChart(value, max);
  }
</script>

<div class="gauge-container">
  <div class="text-xs text-dark-muted uppercase tracking-wider mb-2">{label}</div>
  <div class="relative" style="height: 120px;">
    <canvas bind:this={canvas}></canvas>
    <div class="absolute inset-0 flex items-center justify-center">
      <div class="text-center">
        <div class="text-2xl font-mono font-light text-dark-text">
          {value}{unit}
        </div>
        <div class="text-xs text-dark-muted">
          of {max}{unit}
        </div>
      </div>
    </div>
  </div>
</div>

<style>
  .text-dark-text {
    color: #e6e6e6;
  }
  
  .text-dark-muted {
    color: #8c8c8c;
  }
  
  .relative {
    position: relative;
  }
  
  .absolute {
    position: absolute;
  }
  
  .inset-0 {
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
  }
</style>
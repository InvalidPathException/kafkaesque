<script>
  import { onMount, onDestroy } from 'svelte';
  import Chart from 'chart.js/auto';
  
  export let label = "Metric";
  export let value = 0;
  export let unit = "";
  export let trend = null;
  export let trendValue = null;
  export let sparklineData = [];
  
  let sparklineCanvas;
  let sparklineChart;
  
  function formatValue(val) {
    if (val >= 1000000000) return (val / 1000000000).toFixed(1) + 'B';
    if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M';
    if (val >= 1000) return (val / 1000).toFixed(1) + 'K';
    return val.toString();
  }
  
  function createSparkline() {
    if (!sparklineCanvas || sparklineData.length === 0) return;
    
    const ctx = sparklineCanvas.getContext('2d');
    
    sparklineChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: sparklineData.map(() => ''),
        datasets: [{
          data: sparklineData,
          borderColor: trend === 'up' ? '#52c41a' : trend === 'down' ? '#f5222d' : '#1890ff',
          borderWidth: 1.5,
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            display: false
          },
          y: {
            display: false
          }
        },
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            enabled: false
          }
        },
        interaction: {
          intersect: false
        },
        animation: {
          duration: 0
        }
      }
    });
  }
  
  function updateSparkline(data) {
    if (!sparklineChart) return;
    
    sparklineChart.data.datasets[0].data = data;
    sparklineChart.data.datasets[0].borderColor = 
      trend === 'up' ? '#52c41a' : trend === 'down' ? '#f5222d' : '#1890ff';
    sparklineChart.update('none');
  }
  
  onMount(() => {
    if (sparklineData.length > 0) {
      createSparkline();
    }
  });
  
  onDestroy(() => {
    if (sparklineChart) {
      sparklineChart.destroy();
    }
  });
  
  $: formattedValue = formatValue(value);
  $: if (sparklineChart && sparklineData) {
    updateSparkline(sparklineData);
  }
</script>

<div class="panel p-6">
  <div class="text-xs text-dark-muted uppercase tracking-wider mb-2">
    {label}
  </div>
  
  <div class="flex items-start justify-between">
    <div class="flex-1">
      <div class="flex items-baseline">
        <span class="text-3xl font-light text-dark-text">
          {formattedValue}
        </span>
        {#if unit}
          <span class="text-lg text-dark-muted ml-2">
            {unit}
          </span>
        {/if}
      </div>
      
      {#if trend && trendValue}
        <div class="mt-3 flex items-center text-xs">
          {#if trend === 'up'}
            <svg class="w-4 h-4 text-accent-green mr-1" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M5.293 9.707a1 1 0 010-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L11 7.414V15a1 1 0 11-2 0V7.414L6.707 9.707a1 1 0 01-1.414 0z" clip-rule="evenodd" />
            </svg>
            <span class="text-accent-green">{trendValue}</span>
          {:else if trend === 'down'}
            <svg class="w-4 h-4 text-accent-red mr-1" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M14.707 10.293a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 111.414-1.414L9 12.586V5a1 1 0 012 0v7.586l2.293-2.293a1 1 0 011.414 0z" clip-rule="evenodd" />
            </svg>
            <span class="text-accent-red">{trendValue}</span>
          {/if}
          <span class="text-dark-muted ml-1">from last hour</span>
        </div>
      {/if}
    </div>
    
    {#if sparklineData.length > 0}
      <div class="ml-4" style="width: 80px; height: 40px;">
        <canvas bind:this={sparklineCanvas}></canvas>
      </div>
    {/if}
  </div>
</div>

<style>
  .panel {
    background-color: #141414;
    border: 1px solid #262626;
    border-radius: 0.375rem;
  }
  
  .text-dark-text {
    color: #e6e6e6;
  }
  
  .text-dark-muted {
    color: #8c8c8c;
  }
  
  .text-accent-green {
    color: #52c41a;
  }
  
  .text-accent-red {
    color: #f5222d;
  }
</style>
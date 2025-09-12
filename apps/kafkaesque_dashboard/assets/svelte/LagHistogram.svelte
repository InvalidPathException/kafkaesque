<script>
  import { onMount, onDestroy } from 'svelte';
  import Chart from 'chart.js/auto';
  import ChartWrapper from './ChartWrapper.svelte';
  
  export let values = [];
  export let labels = [];
  
  let canvas;
  let chart;
  
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 0
    },
    scales: {
      x: {
        grid: {
          display: false
        },
        ticks: {
          color: '#e6e6e6',
          maxRotation: 45,
          minRotation: 0
        }
      },
      y: {
        min: 0,
        max: 6000,
        grid: {
          color: '#262626',
          drawBorder: false
        },
        ticks: {
          color: '#e6e6e6',
          stepSize: 1000,
          callback: function(value) {
            if (value >= 1000) return (value/1000).toFixed(1) + 'K';
            return value;
          }
        }
      }
    },
    plugins: {
      legend: {
        display: false
      },
      tooltip: {
        backgroundColor: '#141414',
        titleColor: '#e6e6e6',
        bodyColor: '#e6e6e6',
        borderColor: '#262626',
        borderWidth: 1,
        cornerRadius: 4,
        displayColors: false,
        callbacks: {
          label: function(context) {
            return 'Lag: ' + context.parsed.y.toLocaleString() + ' messages';
          }
        }
      }
    }
  };
  
  function updateChart(newValues, newLabels) {
    if (!chart) return;
    
    chart.data.labels = newLabels || [];
    chart.data.datasets[0].data = newValues || [];
    chart.data.datasets[0].backgroundColor = (newValues || []).map(v => 
      v > 3000 ? '#f5222d' : v > 1000 ? '#faad14' : '#52c41a'
    );
    chart.update('none');
  }
  
  onMount(() => {
    const ctx = canvas.getContext('2d');
    
    chart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: labels || [],
        datasets: [{
          label: 'Consumer Lag',
          data: values || [],
          backgroundColor: (values || []).map(v => 
            v > 3000 ? '#f5222d' : v > 1000 ? '#faad14' : '#52c41a'
          ),
          borderWidth: 0,
          borderRadius: 4
        }]
      },
      options: chartOptions
    });
  });
  
  onDestroy(() => {
    if (chart) {
      chart.destroy();
    }
  });
  
  $: if (chart && values && labels) {
    updateChart(values, labels);
  }
</script>

<ChartWrapper subtitle="Total lag per consumer group">
  <canvas bind:this={canvas}></canvas>
</ChartWrapper>


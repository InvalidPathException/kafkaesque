<script>
  import { onMount, onDestroy } from 'svelte';
  import Chart from 'chart.js/auto';
  import ChartWrapper from './ChartWrapper.svelte';
  
  export let data = [];
  
  let canvas;
  let chart;
  
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 250
    },
    scales: {
      x: {
        type: 'category',
        grid: {
          color: '#262626',
          drawBorder: false
        },
        ticks: {
          color: '#8c8c8c',
          maxRotation: 0,
          autoSkipPadding: 50
        }
      },
      y: {
        grid: {
          color: '#262626',
          drawBorder: false
        },
        ticks: {
          color: '#8c8c8c',
          callback: function(value) {
            if (value >= 1000) return (value/1000) + 'k';
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
            return context.parsed.y + ' msg/s';
          }
        }
      }
    }
  };
  
  function updateChart(newData) {
    if (!chart) return;
    
    const labels = newData.map((_, i) => {
      if (i === 0) return '30m ago';
      if (i === Math.floor(newData.length / 2)) return '15m ago';
      if (i === newData.length - 1) return 'Now';
      return '';
    });
    
    chart.data.labels = labels;
    chart.data.datasets[0].data = newData;
    chart.update('none');
  }
  
  onMount(() => {
    const ctx = canvas.getContext('2d');
    
    chart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          data: data,
          borderColor: '#1890ff',
          backgroundColor: 'rgba(24, 144, 255, 0.1)',
          borderWidth: 2,
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          pointBackgroundColor: '#1890ff',
          pointBorderColor: '#141414',
          pointBorderWidth: 2
        }]
      },
      options: chartOptions
    });
    
    updateChart(data);
  });
  
  onDestroy(() => {
    if (chart) {
      chart.destroy();
    }
  });
  
  $: if (chart && data) {
    updateChart(data);
  }
</script>

<ChartWrapper height="200px">
  <canvas bind:this={canvas}></canvas>
</ChartWrapper>

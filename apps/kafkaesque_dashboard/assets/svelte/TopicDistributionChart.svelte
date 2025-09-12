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
    cutout: '60%',
    plugins: {
      legend: {
        position: 'right',
        labels: {
          color: '#e6e6e6',
          padding: 12,
          font: {
            size: 11,
            family: 'Inter, system-ui, sans-serif',
            weight: 'normal'
          },
          usePointStyle: true,
          pointStyle: 'circle',
          generateLabels: function(chart) {
            const data = chart.data;
            if (!data.labels || !data.datasets[0]) return [];
            
            const total = data.datasets[0].data.reduce((a, b) => a + b, 0);
            return data.labels.map((label, i) => {
              const value = data.datasets[0].data[i];
              const percent = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
              return {
                text: `${label} (${percent}%)`,
                fillStyle: data.datasets[0].backgroundColor[i],
                strokeStyle: data.datasets[0].backgroundColor[i],
                fontColor: '#e6e6e6',
                lineWidth: 0,
                hidden: false,
                index: i
              };
            });
          }
        }
      },
      tooltip: {
        backgroundColor: '#141414',
        titleColor: '#e6e6e6',
        bodyColor: '#e6e6e6',
        borderColor: '#262626',
        borderWidth: 1,
        cornerRadius: 4,
        callbacks: {
          label: function(context) {
            const label = context.label || '';
            const value = context.parsed;
            return label + ': ' + value.toLocaleString() + ' MB';
          }
        }
      }
    }
  };
  
  const colors = [
    '#1890ff',
    '#52c41a',
    '#faad14',
    '#f5222d',
    '#722ed1',
    '#13c2c2',
    '#fa8c16',
    '#a0d911',
    '#eb2f96',
    '#fadb14'
  ];
  
  function updateChart(newValues, newLabels) {
    if (!chart) return;
    
    chart.data.labels = newLabels;
    chart.data.datasets[0].data = newValues;
    chart.update('none');
  }
  
  onMount(() => {
    const ctx = canvas.getContext('2d');
    
    chart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: labels,
        datasets: [{
          data: values,
          backgroundColor: colors,
          borderWidth: 2,
          borderColor: '#0a0a0a',
          hoverBorderColor: '#e6e6e6',
          hoverBorderWidth: 2
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

<ChartWrapper subtitle="Storage size per topic (MB)">
  <canvas bind:this={canvas}></canvas>
</ChartWrapper>


<script>
  export let subtitle = "";
  export let height = "320px";
</script>

<div class="chart-wrapper">
  <div class="chart-content" style="height: {height};">
    <slot />
  </div>
  
  {#if subtitle}
    <div class="chart-footer">
      <div class="chart-subtitle">{subtitle}</div>
    </div>
  {/if}
</div>

<style>
  .chart-wrapper {
    width: 100%;
  }
  
  .chart-content {
    position: relative;
  }
  
  .chart-footer {
    margin-top: 0.5rem;
  }
  
  .chart-subtitle {
    font-size: 0.75rem;
    color: #8c8c8c;
    text-align: center;
  }
</style>
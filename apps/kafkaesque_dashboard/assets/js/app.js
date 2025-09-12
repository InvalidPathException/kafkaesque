import "../css/app.css"
import "phoenix_html"
import {Socket} from "phoenix"
import {LiveSocket} from "phoenix_live_view"

import ThroughputChart from "../svelte/ThroughputChart.svelte"
import ResourceGauge from "../svelte/ResourceGauge.svelte"
import LagHistogram from "../svelte/LagHistogram.svelte"
import MetricCard from "../svelte/MetricCard.svelte"
import TopicDistributionChart from "../svelte/TopicDistributionChart.svelte"
import Chart from 'chart.js/auto'

Chart.defaults.color = '#e6e6e6';
Chart.defaults.borderColor = '#262626';

document.documentElement.classList.add('dark')
const components = {
  ThroughputChart,
  ResourceGauge,
  LagHistogram,
  MetricCard,
  TopicDistributionChart
}

const SvelteHook = {
  mounted() {
    const componentName = this.el.getAttribute("data-name");
    const props = JSON.parse(this.el.getAttribute("data-props") || "{}");
    
    if (!componentName) {
      console.error("Component name must be provided");
      return;
    }
    
    const Component = components[componentName];
    
    if (!Component) {
      console.error(`Unable to find ${componentName} component.`);
      return;
    }
    
    try {
      this.component = new Component({
        target: this.el,
        props: props
      });
    } catch (error) {
      console.error(`Error mounting component ${componentName}:`, error);
    }
  },
  
  updated() {
    const props = JSON.parse(this.el.getAttribute("data-props") || "{}");
    if (this.component && this.component.$set) {
      this.component.$set(props);
    }
  },
  
  destroyed() {
    if (this.component && this.component.$destroy) {
      this.component.$destroy();
    }
  }
};

const csrfToken = document.querySelector("meta[name='csrf-token']")?.getAttribute("content");
const liveSocket = new LiveSocket("/live", Socket, {
  params: {_csrf_token: csrfToken},
  hooks: { SvelteHook }
});

liveSocket.connect();
window.liveSocket = liveSocket;
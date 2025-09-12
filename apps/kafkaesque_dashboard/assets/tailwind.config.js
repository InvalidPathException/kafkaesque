module.exports = {
  content: [
    "./js/**/*.js",
    "../lib/*_web.ex",
    "../lib/*_web/**/*.*ex",
    "../lib/kafkaesque_dashboard/**/*.*ex"
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        'dark': {
          'bg': '#0a0a0a',
          'panel': '#141414',
          'border': '#262626',
          'hover': '#1a1a1a',
          'text': '#e6e6e6',
          'muted': '#8c8c8c',
        },
        'accent': {
          'green': '#52c41a',
          'yellow': '#faad14',
          'red': '#f5222d',
          'blue': '#1890ff',
          'purple': '#722ed1',
          'cyan': '#13c2c2',
        },
        'graph': {
          'green': '#73d13d',
          'blue': '#40a9ff',
          'purple': '#b37feb',
          'yellow': '#ffc53d',
          'red': '#ff7875',
          'cyan': '#36cfc9',
        }
      },
      fontFamily: {
        'mono': ['JetBrains Mono', 'SF Mono', 'Monaco', 'Inconsolata', 'Fira Code', 'monospace'],
        'sans': ['Inter', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'sans-serif'],
      },
      fontSize: {
        'metric': ['2.5rem', { lineHeight: '1', fontWeight: '300' }],
        'metric-lg': ['3.5rem', { lineHeight: '1', fontWeight: '200' }],
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
      spacing: {
        '18': '4.5rem',
        '88': '22rem',
        '128': '32rem',
      },
      boxShadow: {
        'panel': '0 1px 3px 0 rgba(0, 0, 0, 0.5)',
        'panel-hover': '0 2px 8px 0 rgba(0, 0, 0, 0.7)',
      }
    },
  },
  plugins: [
    require('@tailwindcss/forms')({
      strategy: 'class',
    }),
  ],
}
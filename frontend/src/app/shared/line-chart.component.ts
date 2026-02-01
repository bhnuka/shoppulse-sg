import { Component, ElementRef, Input, OnChanges, OnDestroy, SimpleChanges, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Chart, ChartConfiguration, registerables } from 'chart.js';

Chart.register(...registerables);

@Component({
  selector: 'app-line-chart',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './line-chart.component.html',
  styleUrl: './line-chart.component.css'
})
export class LineChartComponent implements OnChanges, OnDestroy {
  @Input() labels: string[] = [];
  @Input() values: number[] = [];
  @Input() title = '';

  @ViewChild('canvas', { static: true }) canvas!: ElementRef<HTMLCanvasElement>;
  private chart?: Chart;

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['labels'] || changes['values']) {
      this.renderChart();
    }
  }

  private renderChart(): void {
    if (this.chart) {
      this.chart.destroy();
    }

    const config: ChartConfiguration<'line'> = {
      type: 'line',
      data: {
        labels: this.labels,
        datasets: [
          {
            label: this.title || 'New entities',
            data: this.values,
            borderColor: '#0f172a',
            backgroundColor: 'rgba(15, 23, 42, 0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: 2
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false }
        },
        scales: {
          x: { grid: { display: false } },
          y: { grid: { color: 'rgba(15, 23, 42, 0.08)' } }
        }
      }
    };

    this.chart = new Chart(this.canvas.nativeElement, config);
  }

  ngOnDestroy(): void {
    if (this.chart) {
      this.chart.destroy();
    }
  }
}

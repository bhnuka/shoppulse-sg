import { Routes } from '@angular/router';
import { OverviewPage } from './pages/overview/overview.page';
import { MapPage } from './pages/map/map.page';
import { IndustriesPage } from './pages/industries/industries.page';
import { EntitiesPage } from './pages/entities/entities.page';
import { ChatPage } from './pages/chat/chat.page';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'overview' },
  { path: 'overview', component: OverviewPage },
  { path: 'footprint', component: MapPage },
  { path: 'industries', component: IndustriesPage },
  { path: 'entities', component: EntitiesPage },
  { path: 'ask', component: ChatPage },
  { path: '**', redirectTo: 'overview' }
];

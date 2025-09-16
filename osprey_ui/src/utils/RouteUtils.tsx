import { Entity } from '../types/EntityTypes';
import { SavedQuery } from '../types/QueryTypes';

export function makeEntityRoute(entity: Entity): string {
  return `/entity/${encodeURIComponent(entity.type)}/${encodeURIComponent(entity.id)}`;
}

export function makeSavedQueryRoute(savedQuery: SavedQuery): string {
  return `/saved-query/${savedQuery.id}`;
}

export function makeLatestSavedQueryRoute(savedQuery: SavedQuery): string {
  return `/saved-query/${savedQuery.id}/latest`;
}

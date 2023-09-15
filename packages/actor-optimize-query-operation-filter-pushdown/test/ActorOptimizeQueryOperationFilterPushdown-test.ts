import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationFilterPushdown } from '../lib/ActorOptimizeQueryOperationFilterPushdown';

describe('ActorOptimizeQueryOperationFilterPushdown', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationFilterPushdown instance', () => {
    let actor: ActorOptimizeQueryOperationFilterPushdown;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationFilterPushdown({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});

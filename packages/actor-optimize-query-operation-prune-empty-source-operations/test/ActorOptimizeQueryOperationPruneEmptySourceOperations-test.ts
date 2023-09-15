import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationPruneEmptySourceOperations } from '../lib/ActorOptimizeQueryOperationPruneEmptySourceOperations';

describe('ActorOptimizeQueryOperationPruneEmptySourceOperations', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationPruneEmptySourceOperations instance', () => {
    let actor: ActorOptimizeQueryOperationPruneEmptySourceOperations;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationPruneEmptySourceOperations({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});

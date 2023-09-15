import { Bus } from '@comunica/core';
import { ActorQueryOperationSource } from '../lib/ActorQueryOperationSource';

describe('ActorQueryOperationSource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQueryOperationSource instance', () => {
    let actor: ActorQueryOperationSource;

    beforeEach(() => {
      actor = new ActorQueryOperationSource({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});

import { Bus } from '@comunica/core';
import { ActorQuerySourceIdentifyHypermediaQpf } from '../lib/ActorQuerySourceIdentifyHypermediaQpf';

describe('ActorQuerySourceIdentifyHypermediaQpf', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceIdentifyHypermediaQpf instance', () => {
    let actor: ActorQuerySourceIdentifyHypermediaQpf;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyHypermediaQpf({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});

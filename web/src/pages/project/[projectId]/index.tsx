import { type GetServerSideProps } from "next";

type Props = {};

export const getServerSideProps: GetServerSideProps<Props> = async (ctx) => {
  const { projectId } = ctx.params ?? {};

  return {
    redirect: {
      destination: `/project/${projectId as string}/traces`,
      permanent: false,
    },
  };
};

export default function ProjectHomeRedirect() {
  return null;
}
